# Item Pipelines

import datetime
import re
import os
from urllib.parse import urlparse
import logging
import json

from itemadapter import ItemAdapter

from scrapy.exceptions import DropItem

from documentcloud.constants import SUPPORTED_EXTENSIONS

from .corrections import corrections
from .log import SilentDropItem


class ParseDatePipeline:
    """Parse dates from scraped data."""

    def process_item(self, item, spider):
        """Parses date from the extracted string."""

        # Publication date

        publication_dt = datetime.datetime.strptime(
            item["publication_lastmodified"], "%a, %d %b %Y %H:%M:%S %Z"
        )

        item["publication_timestamp"] = publication_dt.isoformat() + "Z"

        item["publication_date"] = publication_dt.strftime("%Y-%m-%d")
        item["publication_time"] = publication_dt.strftime("%H:%M:%S UTC")

        item["publication_datetime"] = (
            item["publication_date"] + " " + item["publication_time"]
        )

        # Decision date

        # if not item["decision_date_string"].lower() == "error":
        #     decision_dt = dateparser.parse(
        #         item["decision_date_string"], languages=["fr"]
        #     )
        #     if decision_dt:
        #         item["decision_date"] = decision_dt.strftime("%Y-%m-%d")

        #     else:
        #         item["decision_date"] = "ERROR"

        return item


class CategoryPipeline:
    """Attributes the final category of the document."""

    def process_item(self, item, spider):
        if item["category_local"] == "Avis rendus":
            if (
                "cadrage préalable" in item["project"].lower()
                or "cadrage prealable" in item["project"].lower()
            ):
                item["category"] = "Cadrage"
            else:
                item["category"] = "Avis"

        elif item["category_local"].startswith("Décisions de cas par cas"):
            item["category"] = "Cas par cas"

        elif item["category_local"] == "Saisines":
            item["category"] = "Avis"

        return item


class SourceFilenamePipeline:
    """Adds the source_filename field based on source_file_url."""

    def process_item(self, item, spider):

        path = urlparse(item["source_file_url"]).path

        item["source_filename"] = os.path.basename(path)

        return item


class UnsupportedFiletypePipeline:

    def process_item(self, item, spider):

        filename, file_extension = os.path.splitext(item["source_filename"])
        file_extension = file_extension.lower()

        if file_extension not in SUPPORTED_EXTENSIONS:
            # Drop the item
            raise DropItem("Unsupported filetype")
        else:
            return item


class BeautifyPipeline:
    def process_item(self, item, spider):
        """Beautify & harmonize project & title names."""

        # Project
        item["project"] = item["project"].strip()
        item["project"] = item["project"].replace(" ", " ").replace("’", "'")
        item["project"] = item["project"].rstrip(".,")

        # remove_at_start = [
        #     "Absence de nécessité de réaliser une évaluation environnementale de la ",
        #     # "Cadrage préalable du ",
        # ]
        # for start in remove_at_start:
        #     if item["project"].startswith(start):
        #         item["project"] = item["project"][len(start) :]

        # item["project"] = item["project"].strip()
        item["project"] = item["project"][0].capitalize() + item["project"][1:]

        return item


class UploadLimitPipeline:
    """Sends the signal to close the spider once the upload limit is attained."""

    def open_spider(self, spider):
        self.number_of_docs = 0

    def process_item(self, item, spider):
        self.number_of_docs += 1

        if spider.upload_limit == 0 or self.number_of_docs < spider.upload_limit + 1:
            return item
        else:
            spider.upload_limit_attained = True
            raise SilentDropItem("Upload limit exceeded.")


class CorrectionsPipeline:
    """Manually correct problematic documents listed in corrections.py"""

    def process_item(self, item, spider):

        url = item["source_file_url"]
        if url in corrections:
            # print(f"Found a correction to do for {url}")

            for k, v in corrections[url].items():
                # print(f"replacing {k} with value {v}")
                item[k] = v

        return item


class HandleErrorsPipeline:
    """Pass docs with errors to private"""

    def process_item(self, item, spider):

        if (
            item["project"].lower() == "error"
            # or item["petitioner"].lower() == "error"
            # or item["decision_date_string"].lower() == "error"
            # or item["decision_date"].lower() == "error"
            or "error" in item["title"].lower()
        ):
            item["error"] = True
            item["access"] = "private"
            spider.logger.warn(
                f"Document with error: {item['title']} on {item['source_page_url']}"
            )
            print(item)
        else:
            item["error"] = False
            item["access"] = spider.access_level

        return item


class UploadPipeline:
    """Upload document to DocumentCloud & store event data."""

    def open_spider(self, spider):
        documentcloud_logger = logging.getLogger("documentcloud")
        documentcloud_logger.setLevel(logging.WARNING)

        if not spider.dry_run:
            try:
                spider.logger.info("Loading event data from DocumentCloud...")
                spider.event_data = spider.load_event_data()
            except Exception as e:
                raise Exception("Error loading event data").with_traceback(
                    e.__traceback__
                )
                sys.exit(1)
        else:
            # Load from json if present
            try:

                with open("event_data.json", "r") as file:
                    spider.logger.info("Loading event data from local JSON file...")
                    data = json.load(file)
                    spider.event_data = data
            except:
                spider.event_data = None

        if spider.event_data:
            spider.logger.info(
                f"Loaded event data ({len(spider.event_data)} documents)"
            )
        else:
            spider.logger.info("No event data was loaded.")
            spider.event_data = {}

    def process_item(self, item, spider):

        try:
            if not spider.dry_run:
                spider.client.documents.upload(
                    item["source_file_url"],
                    project=spider.target_project,
                    title=item["title"],
                    description=item["project"],
                    source="www.igedd.developpement-durable.gouv.fr",
                    language="fra",
                    access=item["access"],
                    data={
                        "authority": item["authority"],
                        "category": item["category"],
                        "category_local": item["category_local"],
                        "event_data_key": item["source_file_url"],
                        "publication_date": item["publication_date"],
                        "publication_time": item["publication_time"],
                        "publication_datetime": item["publication_datetime"],
                        "source_scraper": f"IGEDD Scraper {spider.target_year}",
                        "source_file_url": item["source_file_url"],
                        "source_filename": item["source_filename"],
                        "source_page_url": item["source_page_url"],
                        "year": str(item["year"]),
                    },
                )
        except Exception as e:
            raise Exception("Upload error").with_traceback(e.__traceback__)

        else:  # No upload error, add to event_data
            now = datetime.datetime.now().isoformat()
            spider.event_data[item["source_file_url"]] = {
                "last_modified": item["publication_lastmodified"],
                "last_seen": now,
                # "run_id": spider.run_id,
            }
            if spider.run_id:  # only from the web interface
                spider.store_event_data(spider.event_data)

        return item

    def close_spider(self, spider):
        """Update event data when the spider closes."""

        if not spider.dry_run and spider.run_id:
            spider.store_event_data(spider.event_data)
            spider.logger.info(
                f"Uploaded event data ({len(spider.event_data)} documents)"
            )

            if spider.upload_event_data:
                # Upload the event_data to the DocumentCloud interface
                now = datetime.datetime.now()
                timestamp = now.strftime("%Y%m%d_%H%M")
                filename = f"event_data_IGEDD_{timestamp}.json"

                with open(filename, "w+") as event_data_file:
                    json.dump(spider.event_data, event_data_file)
                    spider.upload_file(event_data_file)
                spider.logger.info(
                    f"Uploaded event data to the Documentcloud interface."
                )

        if not spider.run_id:
            with open("event_data.json", "w") as file:
                json.dump(spider.event_data, file)
                spider.logger.info(
                    f"Saved file event_data.json ({len(spider.event_data)} documents)"
                )


class MailPipeline:
    """Send scraping run report."""

    def open_spider(self, spider):
        self.items_ok = []
        self.items_with_error = []

    def process_item(self, item, spider):

        if item["error"] == True:
            self.items_with_error.append(item)
        else:
            self.items_ok.append(item)

        return item

    def close_spider(self, spider):

        def print_item(item, error=False):
            item_string = f"""
            title: {item["title"]}
            project: {item["project"]}
            authority: {item["authority"]}
            category: {item["category"]}
            category_local: {item["category_local"]}
            publication_date: {item["publication_date"]}
            source_file_url: {item["source_file_url"]}
            source_page_url: {item["source_page_url"]}
            year: {item["year"]}
            """

            if error:
                item_string = item_string + f"\nfull_info: {item['full_info']}"

            return item_string

        subject = f"IGEDD Scraper {str(spider.target_year)} (Errors: {len(self.items_with_error)} | New: {len(self.items_ok)}) [{spider.run_name}]"

        errors_content = f"ERRORS ({len(self.items_with_error)})\n\n" + "\n\n".join(
            [print_item(item, error=True) for item in self.items_with_error]
        )

        ok_content = f"SCRAPED ITEMS ({len(self.items_ok)})\n\n" + "\n\n".join(
            [print_item(item) for item in self.items_ok]
        )

        start_content = f"IGEDD Scraper Addon Run {spider.run_id}"

        content = "\n\n".join([start_content, errors_content, ok_content])

        if not spider.dry_run:
            spider.send_mail(subject, content)
