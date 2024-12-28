import json
import random
import datetime as plain_datetime
import copy
from datetime import datetime
import os
import uuid

# load the variables
device_details = [{"location_name": "Beach House", "device_type": "Bulb", "device_name": "Bedroom Light",
                   "device_power_in_watts": 60},
                  {"location_name": "Beach House", "device_type": "Tube Light", "device_name": "Hall Light",
                   "device_power_in_watts": 50},
                  {"location_name": "Beach House", "device_type": "Air Cooler", "device_name": "Garage Air Cooler",
                   "device_power_in_watts": 550},
                  {"location_name": "Chennai House", "device_type": "Fan", "device_name": "Kitchen Fan",
                   "device_power_in_watts": 100},
                  {"location_name": "Chennai House", "device_type": "Television", "device_name": "Dining Area TV",
                   "device_power_in_watts": 150},
                  {"location_name": "Chennai House", "device_type": "Vacuum Cleaner", "device_name": "Dining Area TV",
                   "device_power_in_watts": 150},
                  {"location_name": "Showroom", "device_type": "Air Conditioner", "device_name": "Bedroom AC",
                   "device_power_in_watts": 600},
                  {"location_name": "Showroom", "device_type": "Security Camera", "device_name": "Store Room Camera",
                   "device_power_in_watts": 40},
                  {"location_name": "Showroom", "device_type": "Security Camera", "device_name": "Main Area Camera ",
                   "device_power_in_watts": 40},
                  {"location_name": "Showroom", "device_type": "Bulb", "device_name": "Store Room Light",
                   "device_power_in_watts": 80},
                  {"location_name": "Town House", "device_type": "Bulb", "device_name": "Rest Room Light",
                   "device_power_in_watts": 90},
                  {"location_name": "Town House", "device_type": "Heater", "device_name": "Bath Heater",
                   "device_power_in_watts": 200},
                  {"location_name": "Town House", "device_type": "Fan", "device_name": "Ventilation Fan",
                   "device_power_in_watts": 85},
                  {"location_name": "Condominium", "device_type": "Fan", "device_name": "Table Fan",
                   "device_power_in_watts": 110},
                  {"location_name": "Condominium", "device_type": "Stove", "device_name": "Electric Stove",
                   "device_power_in_watts": 120},
                  {"location_name": "Condominium", "device_type": "Dryer", "device_name": "Hair Dryer",
                   "device_power_in_watts": 95}]


def check_key_in_array(array, key):
    # Iterate over each dictionary in the array
    for obj in array:
        if key in obj:
            return True
    return False


def data_generator(no_of_days, no_of_records_per_day):
    final_result = []  # to store the final results
    turned_on_devices = {}  # to keep the devices already turned on
    turned_off_devices = {} # to keep the devices turned off to make sure it is not turned on twice
    start_date = "2024-09-"
    for d in range(1, no_of_days+1):
        print("d:%s", d)
        picked_date = start_date + (("0" + str(d)) if d < 10 else str(d))
        for x in range(no_of_records_per_day):
            random_element = random.choice(copy.deepcopy(device_details))
            random_element["id"] = str(uuid.uuid4())
            status_device_key = random_element["location_name"] + random_element["device_type"] + random_element[
                "device_name"]
            if status_device_key in turned_on_devices:
                turned_on_device_op_time = turned_on_devices[status_device_key].split("|")[0]
                turned_on_device_session_id = turned_on_devices[status_device_key].split("|")[1]
                on_operation_date = turned_on_device_op_time.split()[0]
                on_operation_time = turned_on_device_op_time.split()[1].split(":")
                on_operation_hour = int(on_operation_time[0])
                on_operation_minute = int(on_operation_time[1])
                on_operation_second = int(on_operation_time[2])
                is_continue = True
                while is_continue:
                    new_hour = random.randint(0, 23)
                    new_minute = random.randint(0, 59)
                    new_second = random.randint(0, 59)
                    if on_operation_date != picked_date or new_hour > on_operation_hour or (
                            new_hour == on_operation_hour and new_minute > on_operation_minute) or (
                            new_hour == on_operation_hour and new_minute == on_operation_minute and new_second > on_operation_second):
                        random_element["operation_time"] = picked_date + " " + str(
                            plain_datetime.time(new_hour, new_minute, new_second))
                        random_element["change_of_status"] = 0
                        random_element["session_id"] = turned_on_device_session_id
                        is_continue = False
                    else:
                        continue
                turned_off_devices[
                    random_element["location_name"] + random_element["device_type"] + random_element["device_name"]] = \
                    random_element["operation_time"]
                del turned_on_devices[status_device_key]
            else:
                if status_device_key in turned_off_devices:
                    turned_off_device_op_time = turned_off_devices[status_device_key]
                    off_operation_date = turned_off_device_op_time.split()[0]
                    off_operation_time = turned_off_device_op_time.split()[1].split(":")
                    off_operation_hour = int(off_operation_time[0])
                    off_operation_minute = int(off_operation_time[1])
                    off_operation_second = int(off_operation_time[2])
                    is_continue = True
                    while is_continue:
                        new_hour = random.randint(0, 23)
                        new_minute = random.randint(0, 59)
                        new_second = random.randint(0, 59)
                        if off_operation_date != picked_date or new_hour > off_operation_hour or (
                                new_hour == off_operation_hour and new_minute > off_operation_minute) or (
                                new_hour == off_operation_hour and new_minute == off_operation_minute and new_second > off_operation_second):
                            random_element["operation_time"] = picked_date + " " + str(
                                plain_datetime.time(new_hour, new_minute, new_second))
                            random_element["change_of_status"] = 1
                            random_element["session_id"] = str(uuid.uuid4())
                            is_continue = False
                        else:
                            continue
                    del turned_off_devices[status_device_key]
                else:
                    new_hour = random.randint(0, 23)
                    new_minute = random.randint(0, 59)
                    new_second = random.randint(0, 59)
                    random_element["operation_time"] = picked_date + " " + str(
                        plain_datetime.time(new_hour, new_minute, new_second))
                    random_element["change_of_status"] = 1
                    random_element["session_id"] = str(uuid.uuid4())

                turned_on_devices[
                    random_element["location_name"] + random_element["device_type"] + random_element["device_name"]] = \
                    random_element["operation_time"] + "|" + random_element["session_id"]

            final_result.append(random_element)

    # sort by date
    sorted_result = sorted(final_result, key=lambda x: datetime.strptime(x["operation_time"], "%Y-%m-%d %H:%M:%S"))

    write_result(sorted_result)

def write_result(sorted_result):
    # Specify the file name
    filename = "target/smart_device_logs-" + datetime.today().strftime('%Y%m%d%H%M%S') + ".log"  # JSON Lines format (one JSON object per line)
    file_dir = "target"

    # Create a directory if it doesn't exist
    if not os.path.exists(file_dir):
        os.mkdir(file_dir)

    if os.path.isfile(filename):
        os.remove(filename)
    # Write the array of JSON objects to a file
    with open(filename, 'w') as f:
        for item in sorted_result:
            json_line = json.dumps(item)  # Convert dictionary to JSON string
            f.write(json_line + '\n')  # Write the JSON string followed by a newline

    print(f"Data has been written to {filename}")

data_generator(30, 35)
