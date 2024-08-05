import ijson
import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(filename='jsonDataConversion.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# Record the start time
start_time = time.time()

# Path to the JSON file
json_file_path = "./dataset/data.json"

# Output file
output_file_path = "./dataset/out/Restaurent.txt"

# Normalize data
def normalize_text(text):
    return text.strip().lower() if isinstance(text, str) else text

# Function to process each city data
def process_city_data(city, city_data, file):
    try:
        vcitycolval = normalize_text(city)
        vlinkcolval = normalize_text(city_data.get("link", ""))
        
        if "restaurants" in city_data and isinstance(city_data["restaurants"], dict):
            for restau_code, restaurant_details in city_data["restaurants"].items():
                vresturentcolval = normalize_text(restau_code)
                vname = normalize_text(restaurant_details.get("name", ""))
                vrating = normalize_text(restaurant_details.get("rating", ""))
                vrating_count = normalize_text(restaurant_details.get("rating_count", ""))
                vaddress = normalize_text(restaurant_details.get("address", ""))
                vcuisine = normalize_text(restaurant_details.get("cuisine", ""))
                vlic_no = normalize_text(restaurant_details.get("lic_no", ""))
                
                if "menu" in restaurant_details and isinstance(restaurant_details["menu"], dict):
                    for menukey, menuval in restaurant_details["menu"].items():
                        vMenuCatval = normalize_text(menukey)
                        
                        if isinstance(menuval, dict):
                            for menucartkey, menucartval in menuval.items():
                                vmenucartval = normalize_text(menucartkey)
                                vmenucartprice = normalize_text(menucartval.get('price', ''))
                                vmenuvegtype = normalize_text(menucartval.get('veg_or_non_veg', ''))

                                record = (f"{vcitycolval}|{vlinkcolval}|{vresturentcolval}|{vname}|"
                                          f"{vrating}|{vrating_count}|{vaddress}|{vcuisine}|{vlic_no}|"
                                          f"{vMenuCatval}|{vmenucartval}|{vmenucartprice}|{vmenuvegtype}")
                                file.write(record + '\n')
    except Exception as e:
        logging.error(f"Error processing city {city}: {e}")

# Read and process the JSON file
try:
    with open(json_file_path, 'r', encoding='utf-8') as json_file:
        data = ijson.kvitems(json_file, '')
        
        with open(output_file_path, 'w', encoding='utf-8') as file:
            file.write("City|Link|RestaurentID|RestaurentName|Rating|Rating_Count|Address|Cuisine|Loc_No|MenuCategory|MenuCartItem|MenuCartItemPrice|MenuCartItemType" + '\n')
            for city, city_data in data:
                logging.info(f"Processing city: {city}")
                process_city_data(city, city_data, file)
except FileNotFoundError as e:
    logging.error(f"File not found: {e}")
except json.JSONDecodeError as e:
    logging.error(f"Error decoding JSON: {e}")
except Exception as e:
    logging.error(f"Unexpected error: {e}")

# Record the end time and calculate the duration
end_time = time.time()
logging.info(f"Total runtime: {end_time - start_time} seconds")
