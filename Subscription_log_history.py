"""This script was created to help the Success org address subscription status inquiries from clients.
It's designed to support the Success org in answering questions related to when a profile became suppressed,
if a profile was ever suppressed, who unsubscribed/suppressed the profile, etc."""

import os
import csv
import requests
from dotenv import load_dotenv

load_dotenv()


# get all profiles in an account and paginate through each page to retrieve the next set of profiles
def get_profiles():
    baseurl = "https://a.klaviyo.com/api/profiles/?page[size]=100"
    headers = {
        "accept": "application/json",
        "revision": "2023-02-22",
        "Authorization": f"Klaviyo-API-Key {os.getenv('KLAVIYO_PRIVATE_KEY')}"
    }

    response = requests.get(baseurl, headers=headers)
    response_data = response.json()["data"]
    next_url = response.json()["links"].get("next", "")

    counter = 1
    while next_url:
        counter += 1
        print(f"processing page {counter}")

        response = requests.get(next_url, headers=headers)
        response_data += response.json()["data"]
        next_url = response.json()["links"].get("next", "")
    return response_data


# filter through the retrieved JSON data to extract profile id, subscription date, consent status, first name and email
def process_profile_data(retrieve_profile_data):
    filtered_profile_data = []

    for retrieve_profile in retrieve_profile_data:
        filtered_profile_data.append({
            "profile_id": retrieve_profile["id"],
            "subscription_updated_date": retrieve_profile.get("attributes", {}).get("subscriptions", {}).get("email",
                                                                                                             {}).get(
                "marketing", {}).get("timestamp", ""),
            "profile_email_consent": retrieve_profile.get("attributes", {}).get("subscriptions", {}).get("email",
                                                                                                         {}).get(
                "marketing", {}).get("consent", ""),
            "included_profile_firstname": retrieve_profile.get("attributes", {}).get("first_name", {}),
            "included_profile_email": retrieve_profile.get("attributes", {}).get("email", {})

        })
    return filtered_profile_data


# saves the profile data that's been retrieved and filtered into a new csv file

def save_filtered_profile_data_as_csv(filtered_profile_subscription_data):
    with open('subscription_events_profile_data.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(filtered_profile_subscription_data[0].keys())
        for filtered_profile in filtered_profile_subscription_data:
            writer.writerow(filtered_profile.values())


# gets all metrics in a given account and returns the data object with all associated metric data
def get_metric_data():
    url = "https://a.klaviyo.com/api/metrics"

    headers = {
        "accept": "application/json",
        "revision": "2023-02-22",
        "Authorization": f"Klaviyo-API-Key {os.getenv('KLAVIYO_PRIVATE_KEY')}"
    }
    response = requests.get(url, headers=headers)
    return response.json()["data"]


# filters through the data object to find the requested metric and returns the metric id
def process_metric_data_for_metric_id(retrieve_metric_data,
                                      metric_name):  # put in an argument that takes in the metric name
    metric_id = ""

    for metric in retrieve_metric_data:
        if metric.get("attributes", {}).get("name", "") == metric_name:  # compare to a supplied name
            metric_id = metric["id"]
            break
    return metric_id


# passes in the metric id and uses that to get the JSON data for all events related to that metric
def get_metric_events(metric_id):
    if not metric_id:
        return []
    baseurl = "https://a.klaviyo.com/api/events/?filter=equals(metric_id,\"" + metric_id + "\")&fields[profile]=first_name,email&include=profiles"
    headers = {
        "accept": "application/json",
        "revision": "2023-02-22",
        "Authorization": f"Klaviyo-API-Key {os.getenv('KLAVIYO_PRIVATE_KEY')}"
    }

    # response = requests.get(url, headers=headers)
    # return response.json()

    response = requests.get(baseurl, headers=headers)
    response_data = response.json().get("data",[])
    next_url = response.json()["links"].get("next", "")

    counter = 1
    while next_url:
        counter += 1
        print(f"processing page {counter}")

        response = requests.get(next_url, headers=headers)
        response_data += response.json().get("data",[])
        next_url = response.json()["links"].get("next", "")
    return response_data


# filters through JSON of specified metric to retrieve profile id, subscription timestamp, consent, first name, email
def filter_specific_metric_data_for_field_data(retrieve_metric_response):
    if not retrieve_metric_response:
        return []

    filtered_metric_data = []
    print(retrieve_metric_response)
    retrieve_metric_data = retrieve_metric_response["data"]
    retrieve_metric_included = retrieve_metric_response["included"]

    for retrieve_metric in retrieve_metric_data:
        data_profile_id = retrieve_metric.get("attributes", {}).get("profile_id", {})

        for retrieve_profile in retrieve_metric_included:
            if data_profile_id == retrieve_profile.get("id"):
                filtered_metric_data.append({
                    "profile_id": retrieve_metric.get("attributes", {}).get("profile_id", {}),
                    "subscription_updated_date": retrieve_metric.get("attributes", {}).get("datetime", {}),
                    "profile_email_consent": "suppressed",
                    "included_profile_firstname": retrieve_profile.get("attributes", {}).get("first_name", ""),
                    "included_profile_email": retrieve_profile.get("attributes", {}).get("email", "")
                })
            break
    return filtered_metric_data


# saves filtered metric event data as a new csv file, separate from the profiles csv file
def save_filtered_data_as_csv(filtered_specific_metric_event_data, filename):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(filtered_specific_metric_event_data[0].keys())
        for filtered_specific_metric in filtered_specific_metric_event_data:
            writer.writerow(filtered_specific_metric.values())


# merges the profile and metric event csv files into a new file
def merge_lists(list_of_lists):
    merged_lists = []
    for data_list in list_of_lists:
        merged_lists += data_list
    return merged_lists


# the first functon in the script that calls all of the above functions to run
def main():
    retrieved_metric_data = get_metric_data()
    # print(retrieved_metric_data)

    # Step 1: Retrieve metric ids for all metrics that lead to consent or suppression
    retrieved_metric_unsubscribe_id = process_metric_data_for_metric_id(retrieved_metric_data, "Unsubscribe")
    # print(retrieved_metric_unsubscribe_id)

    retrieved_metric_subscribe_id = process_metric_data_for_metric_id(retrieved_metric_data, "Subscribe")
    # print(retrieved_metric_subscribe_id)

    retrieved_metric_unsubscribe_from_list_id = process_metric_data_for_metric_id(retrieved_metric_data,
                                                                                  "Unsubscribe from List")
    # print(retrieved_metric_unsubscribe_from_list_id)

    retrieved_metric_marked_spam_id = process_metric_data_for_metric_id(retrieved_metric_data, "Marked Email as Spam")
    # print(retrieved_metric_marked_spam_id)

    retrieved_metric_consented_sms_id = process_metric_data_for_metric_id(retrieved_metric_data,
                                                                          "Consented to Receive SMS")
    # print(retrieved_metric_consented_sms_id)

    retrieved_metric_unsubscribed_from_sms_id = process_metric_data_for_metric_id(retrieved_metric_data,
                                                                                  "Unsubscribed from SMS")
    # print(retrieved_metric_unsubscribed_from_sms_id)

    # Step 2: Get event data for each metric
    retrieved_filter_unsubscribe_metric_data_for_field_data = get_metric_events(retrieved_metric_unsubscribe_id)
    # print(retrieved_filter_unsubscribe_metric_data_for_field_data)

    retrieved_filter_subscribe_metric_data_for_field_data = get_metric_events(retrieved_metric_subscribe_id)
    # print(retrieved_filter_subscribe_metric_data_for_field_data)

    retrieved_filter_unsubscribe_from_list_metric_data_for_field_data = get_metric_events(
        retrieved_metric_unsubscribe_from_list_id)
    # print(retrieved_filter_unsubscribe_from_list_metric_data_for_field_data)

    retrieved_filter_marked_as_spam_metric_data_for_field_data = get_metric_events(
        retrieved_metric_marked_spam_id)
    # print(retrieved_filter_marked_as_spam_metric_data_for_field_data)

    retrieved_filter_consented_sms_metric_data_for_field_data = get_metric_events(
        retrieved_metric_consented_sms_id)
    # print(retrieved_filter_consented_sms_metric_data_for_field_data)

    retrieved_filter_unsubscribed_from_sms_metric_data_for_field_data = get_metric_events(
        retrieved_metric_unsubscribed_from_sms_id)
    # print(retrieved_filter_unsubscribed_from_sms_metric_data_for_field_data)

    # Step 3: Filter event data for consent properties
    filtered_metric_unsubscribe_event_data = filter_specific_metric_data_for_field_data(
        retrieved_filter_unsubscribe_metric_data_for_field_data)
    # print(filtered_metric_unsubscribe_event_data)

    filtered_metric_subscribe_event_data = filter_specific_metric_data_for_field_data(
        retrieved_filter_subscribe_metric_data_for_field_data)
    # print(filtered_metric_subscribe_event_data)

    filtered_metric_unsubscribe_from_list_event_data = filter_specific_metric_data_for_field_data(
        retrieved_filter_unsubscribe_from_list_metric_data_for_field_data)
    # print(filtered_metric_unsubscribe_from_list_event_data)

    filtered_metric_marked_as_spam_event_data = filter_specific_metric_data_for_field_data(
        retrieved_filter_marked_as_spam_metric_data_for_field_data)
    # print(filtered_metric_marked_as_spam_event_data)

    filtered_metric_consented_sms_event_data = filter_specific_metric_data_for_field_data(
        retrieved_filter_consented_sms_metric_data_for_field_data)
    # print(filtered_metric_consented_sms_event_data)

    filtered_metric_unsubscribed_from_sms_event_data = filter_specific_metric_data_for_field_data(
        retrieved_filter_unsubscribed_from_sms_metric_data_for_field_data)
    # print(filtered_metric_unsubscribed_from_sms_event_data)

    # Step 4: Retrieves all profile data
    retrieve_profile_data = get_profiles()
    # print(retrieve_profile_data)

    filtered_profile_subscription_data = process_profile_data(retrieve_profile_data)
    # print(filtered_profile_subscription_data)

    # Step 5: Combines the lists of all the filtered data
    clean_merged_file = merge_lists([
        filtered_profile_subscription_data,
        filtered_metric_unsubscribe_event_data,
        filtered_metric_subscribe_event_data,
        filtered_metric_unsubscribe_from_list_event_data,
        filtered_metric_marked_as_spam_event_data,
        filtered_metric_consented_sms_event_data,
        filtered_metric_unsubscribed_from_sms_event_data
    ])
    # print(clean_merged_file)

    # Step 6: Saves data as a csv
    save_filtered_data_as_csv(clean_merged_file, "clean_merged_data.csv")


main()

# DONE - Step 1 add pagination to the function that is fetching events
# DONE - Step 2 rename the unsubscribe functions so that they are more generic and do not include the word unsubscribe in them
# DONE - Step 3 include subscribe, unsubscribe from list and sms metrics; do the same process for these metrics as I've done for unsubscribe
# DONE - Step 4 update the merge_filtered_profile_subscription_status function so that it includes the generic name for metrics rather than filtered_unsubscribe_data
# Step 5 update the main function so that all variables or functions that start with unsubscribe are replaced with the generic metric name
# Step 5 clean up comments so that they refer to all metrics and not just the unsubscribe events

# TODO: Commenting out bounces for now. Will revisit this when we have a better idea and time to filter bounce events
# retrieved_metric_bounced_id = process_metric_data_for_metric_id(retrieved_metric_data, "Bounced Email")
# print(retrieved_metric_bounced_id)