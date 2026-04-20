import os
import pandas as pd
import logging
from openaq import OpenAQ
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from airflow.models import Variable

THAI_REGIONS_EN = {
    "North": {
        "เชียงใหม่": "Chiang Mai",
        "เชียงราย": "Chiang Rai",
        "น่าน": "Nan",
        "พะเยา": "Phayao",
        "แพร่": "Phrae",
        "แม่ฮ่องสอน": "Mae Hong Son",
        "ลำปาง": "Lampang",
        "ลำพูน": "Lamphun",
        "อุตรดิตถ์": "Uttaradit",
    },
    "Northeast": {
        "กาฬสินธุ์": "Kalasin",
        "ขอนแก่น": "Khon Kaen",
        "ชัยภูมิ": "Chaiyaphum",
        "นครพนม": "Nakhon Phanom",
        "นครราชสีมา": "Nakhon Ratchasima",
        "บึงกาฬ": "Bueng Kan",
        "บุรีรัมย์": "Buri Ram",
        "มหาสารคาม": "Maha Sarakham",
        "มุกดาหาร": "Mukdahan",
        "ยโสธร": "Yasothon",
        "ร้อยเอ็ด": "Roi Et",
        "เลย": "Loei",
        "ศรีสะเกษ": "Si Sa Ket",
        "สกลนคร": "Sakon Nakhon",
        "สุรินทร์": "Surin",
        "หนองคาย": "Nong Khai",
        "หนองบัวลำภู": "Nong Bua Lamphu",
        "อำนาจเจริญ": "Amnat Charoen",
        "อุดรธานี": "Udon Thani",
        "อุบลราชธานี": "Ubon Ratchathani",
    },
    "Central": {
        "กรุงเทพมหานคร": "Bangkok",
        "กรุงเทพ": "Bangkok",
        "กำแพงเพชร": "Kamphaeng Phet",
        "ชัยนาท": "Chai Nat",
        "นครนายก": "Nakhon Nayok",
        "นครปฐม": "Nakhon Pathom",
        "นครสวรรค์": "Nakhon Sawan",
        "นนทบุรี": "Nonthaburi",
        "ปทุมธานี": "Pathum Thani",
        "พระนครศรีอยุธยา": "Phra Nakhon Si Ayutthaya",
        "พิจิตร": "Phichit",
        "พิษณุโลก": "Phitsanulok",
        "เพชรบูรณ์": "Phetchabun",
        "ลพบุรี": "Lop Buri",
        "สมุทรปราการ": "Samut Prakan",
        "สมุทรสงคราม": "Samut Songkhram",
        "สมุทรสาคร": "Samut Sakhon",
        "สระบุรี": "Saraburi",
        "สิงห์บุรี": "Sing Buri",
        "สุโขทัย": "Sukhothai",
        "สุพรรณบุรี": "Suphan Buri",
        "อ่างทอง": "Ang Thong",
        "อุทัยธานี": "Uthai Thani",
    },
    "East": {
        "จันทบุรี": "Chanthaburi",
        "ฉะเชิงเทรา": "Chachoengsao",
        "ชลบุรี": "Chon Buri",
        "ตราด": "Trat",
        "ปราจีนบุรี": "Prachin Buri",
        "ระยอง": "Rayong",
        "สระแก้ว": "Sa Kaeo",
    },
    "West": {
        "กาญจนบุรี": "Kanchanaburi",
        "ตาก": "Tak",
        "ประจวบคีรีขันธ์": "Prachuap Khiri Khan",
        "เพชรบุรี": "Phetchaburi",
        "ราชบุรี": "Ratchaburi",
    },
    "South": {
        "กระบี่": "Krabi",
        "ชุมพร": "Chumphon",
        "ตรัง": "Trang",
        "นครศรีธรรมราช": "Nakhon Si Thammarat",
        "นราธิวาส": "Narathiwat",
        "ปัตตานี": "Pattani",
        "พังงา": "Phangnga",
        "พัทลุง": "Phatthalung",
        "ภูเก็ต": "Phuket",
        "ยะลา": "Yala",
        "ระนอง": "Ranong",
        "สงขลา": "Songkhla",
        "สตูล": "Satun",
        "สุราษฎร์ธานี": "Surat Thani",
    },
}

PROVINCE_MAPPING = {}
for region_en, provinces in THAI_REGIONS_EN.items():
    for prov_th, prov_en in provinces.items():
        PROVINCE_MAPPING[prov_th] = {"province_en": prov_en, "region_en": region_en}


def get_province_en(lat, lon, reverse_geocode):
    try:
        location = reverse_geocode(f"{lat}, {lon}", language="th")
        if location:
            for prov_th, info in PROVINCE_MAPPING.items():
                if prov_th in location.address:
                    return info["province_en"], info["region_en"]
    except:
        pass
    return "Unknown", "Unknown"


def sync_locations(**kwargs):
    logger = logging.getLogger("airflow.task")
    csv_path = kwargs.get("csv_path", "/opt/airflow/data/thai_locations.csv")
    
    api_key = Variable.get("openaq_api_key", default_var=os.getenv("OPENAQ_API_KEY"))
    client = OpenAQ(api_key=api_key)
    
    geolocator = Nominatim(user_agent="thai_pm25_pipeline_v4")
    reverse_geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)

    if os.path.exists(csv_path):
        df_old = pd.read_csv(csv_path)
        existing_ids = set(df_old["location_id"].unique())
    else:
        df_old = pd.DataFrame()
        existing_ids = set()

    logger.info("Fetching latest locations from OpenAQ API")
    res = client.locations.list(countries_id=111, limit=1000)

    new_entries = []
    for loc in res.results:
        if loc.id not in existing_ids and loc.coordinates:
            logger.info(f"Found new location ID: {loc.id}")
            prov, reg = get_province_en(
                loc.coordinates.latitude, loc.coordinates.longitude, reverse_geocode
            )
            
            if prov == "Unknown":
                logger.info(f"Skipping foreign/unknown station: {loc.name}")
                continue

            new_entries.append(
                {
                    "location_id": loc.id,
                    "original_name": loc.name,
                    "latitude": loc.coordinates.latitude,
                    "longitude": loc.coordinates.longitude,
                    "province": prov,
                    "region": reg,
                }
            )

    if new_entries:
        df_new = pd.DataFrame(new_entries)
        df_final = pd.concat([df_old, df_new], ignore_index=True)
        df_final.to_csv(csv_path, index=False)
        logger.info(f"Added {len(new_entries)} new locations to Master Data.")
    else:
        logger.info("Master Data is already up to date.")
