"""

This script aims to fetch images from a specific project website.

This assume that the project website is an independent domain,
it should be a consolidated sub domain of any listing websites.

An example of such website would be https://www.theverandahresidencescondo.sg/

Note:
    This process should not be scheduled, but to be performed on a
    needed basis

"""

import queue as q
from rea_python.main.http_requests import to_soup, get_url
from src.constants import RANDOM_HEADERS, S3_IMAGE_OBJECT_URL_PREFIX, SRC_ROOT
from typing import List
from bs4 import BeautifulSoup
from src.connector.manager import DatabaseManager
import logging
import http.client
import pandas as pd
import io
from rea_python.constants import OutputFormat, DBCopyMode
from rea_python.main.aws import S3Hook
from rea_python.utils import hash_str
import src.config as cfg
import requests
from tqdm import tqdm
import os

http.client._MAXHEADERS = 1000

logger = logging.getLogger(__name__)
S3_IMAGE_FOLDER = "project_image"
S3_FOLDER_NAME = "project_website_image"
QUERY_PATH = os.path.join(SRC_ROOT, "fetch_image", "sql")


def main():
    dm = DatabaseManager()
    dm.pg_hook.load_queries_from_folders(QUERY_PATH)
    dm.rd_hook.load_queries_from_folders(QUERY_PATH)

    projects = dm.rd_hook.execute_loaded_query(query_name="fetch_new_launch")
    result_images = []

    for project in tqdm(projects):
        task_queue = q.Queue()  # initialise a new queue for each project
        processed_list = set()  # to prevent trapped in web tree
        project_name = project.project_name
        corrected_id = project.corrected_id
        seed_url = project.website
        domain = seed_url
        images = process_task(domain, seed_url, task_queue, processed_list)
        while True:
            try:
                new_task = task_queue.get(timeout=5)
            except q.Empty:
                logger.warning("The queue has been depleted. ")
                break

            logger.warning(f"Looking at {new_task}")
            if new_task in processed_list:
                logger.warning("We have parsed this page before. ")
                continue

            iter_images = process_task(
                domain=domain,
                seed_url=new_task,
                task_queue=task_queue,
                processed_list=processed_list,
            )
            if iter_images is not None:
                images.extend(iter_images)
            images = list(set(images))

        if images is not None:
            result_images.extend(
                [(project_name, image, corrected_id) for image in images]
            )

    df = pd.DataFrame(
        result_images, columns=["project_name", "hashed_url", "corrected_project_id"]
    )
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)

    s3 = S3Hook(cfg.IMAGE_S3_BUCKET)
    files = s3.list_files_in_bucket(prefix=S3_IMAGE_FOLDER)
    df["s3_link"] = ""
    df["authorized"] = True
    df["country_id"] = 1

    for ind, row in df.iterrows():
        file_name = row["hashed_url"]
        hashed_url = hash_str(file_name)
        full_s3_path = f"{S3_IMAGE_FOLDER}/{S3_FOLDER_NAME}/{hashed_url}"
        if hashed_url in files:
            logger.warning("Found image, continuing...")
        else:
            try:
                file_as_binary = requests.get(file_name, stream=True)
            except requests.exceptions.ConnectionError:
                logger.warning("Connection error, continuing...")
                continue

            if file_as_binary.status_code == 200:
                s3.upload_file(
                    target_key=full_s3_path,
                    target_binary=io.BytesIO(file_as_binary.content),
                )
        df.at[ind, "s3_link"] = f"{S3_IMAGE_OBJECT_URL_PREFIX}/{full_s3_path}"
        df.at[ind, "hashed_url"] = hashed_url
        logger.warning(f"Uploaded image to {full_s3_path}")

        target_cols = (
            "hashed_url",
            "corrected_project_id",
            "s3_link",
            "authorized",
            "country_id",
        )
        df_temp = df.iloc[[ind], 1:]
        # For saving the progress, we decided to upload the df after every row is being uploaded
        # We could upload the whole df next time
        try:
            dm.pg_hook.copy_from_df(
                df=df_temp,  # to change this to df if we upload the whole df next time
                target_table="reference.manual_project_image",
                mode=DBCopyMode.APPEND,
                target_cols=target_cols,
            )
        except Exception as e:
            logger.warning(f"Exception occurred: {e}")
            continue


def process_task(domain, seed_url, task_queue, processed_list):
    try:
        resp = get_url(seed_url, header=RANDOM_HEADERS)
    except Exception:
        logger.warning("Unable to open this url. skip")
        processed_list.add(seed_url)
        return

    try:
        soup = to_soup(resp.content.decode())
    except UnicodeDecodeError:
        logger.warning("Unable to decode page")
        processed_list.add(seed_url)
        return

    new_seeds = extract_new_urls(soup, domain, seed_url)
    load_seed(task_queue, new_seeds, processed_list)
    processed_list.add(seed_url)
    return extract_images(soup, domain)


def load_seed(queue: q.Queue, seeds: List[str], processed_list: List[str]):
    for seed in seeds:
        if seed not in processed_list:
            queue.put(seed)


def filter_tasks(seed: str, domain: str, seed_url: str):

    if seed is None:
        return False

    if seed[:4] != "http":
        return False

    if domain not in seed:
        return False

    if seed_url == seed:
        return False

    if "whatsapp.com" in seed:
        return False

    return True


def filter_images(image_url: str):
    if image_url is None:
        return False

    if "jpg" in image_url or "png" in image_url:
        return True

    return False


def extract_images(soup: BeautifulSoup, domain: str) -> List[str]:
    images = soup.find_all("img")
    result = []
    for image in images:
        if filter_images(image.get("src")):
            src_link = image.get("src")
            if domain not in src_link:
                if "/" == src_link[0] and "/" == domain[-1]:
                    src_link = domain[:-1] + src_link
                elif "/" == src_link[0] and "/" != domain[-1]:
                    src_link = domain + src_link
                elif "/" != src_link[0] and "/" == domain[-1]:
                    src_link = domain + src_link
                elif "/" != src_link[0] and "/" != domain[-1]:
                    src_link = domain + "/" + src_link
            result.append(src_link)
    return result


def extract_new_urls(soup: BeautifulSoup, domain: str, seed: str) -> List[str]:
    task_links = soup.find_all("a")
    extracted_hrefs = set([a_tag.get("href") for a_tag in task_links])
    for a_tag in extracted_hrefs:
        logger.warning(a_tag)
        logger.warning(f"{filter_tasks(a_tag, domain=domain, seed_url=seed)}")
    valid_tasks = list(
        set(
            [
                a_tag
                for a_tag in extracted_hrefs
                if filter_tasks(a_tag, domain=domain, seed_url=seed)
            ]
        )
    )
    return valid_tasks


if __name__ == "__main__":
    main()
