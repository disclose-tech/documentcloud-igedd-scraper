# Item Pipelines

import datetime
import re
import os
from urllib.parse import urlparse
import logging

import dateparser

from itemadapter import ItemAdapter

from scrapy.exceptions import DropItem


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

        if not item["decision_date_string"] == "ERROR":
            decision_dt = dateparser.parse(
                item["decision_date_string"], languages=["fr"]
            )
            if decision_dt:
                item["decision_date"] = decision_dt.strftime("%Y-%m-%d")

            else:
                item["decision_date"] = "ERROR"

        return item


class CategoryPipeline:
    """Attributes the final category of the document."""

    def process_item(self, item, spider):
        if item["category_local"] == "Avis rendus":
            if "cadrage préalable" in item["project"].lower():
                item["category"] = "Cadrage"
            else:
                item["category"] = "Avis"

        elif item["category_local"].startswith("Décisions de cas par cas"):
            item["category"] = "Cas par cas"

        # elif item["category_local"] in [
        #     "Avis rendus sur plans et programmes",
        #     "Avis rendus sur projets",
        # ]:
        #     item["category"] = "Avis"

        return item


class SourceFilenamePipeline:
    """Adds the source_filename field based on source_file_url."""

    def process_item(self, item, spider):

        path = urlparse(item["source_file_url"]).path

        item["source_filename"] = os.path.basename(path)

        return item


class BeautifyPipeline:
    def process_item(self, item, spider):
        """Beautify & harmonize project & title names."""

        #         # Title
        #         if item["title"].startswith("("):
        #             item["title"] = item["title"][1:]

        #         if item["title"].startswith("la demande"):
        #             item["title"] = "Demande" + item["title"][10:]
        #         elif item["title"].startswith("demande"):
        #             item["title"] = item["title"][0].capitalize() + item["title"][1:]

        # Project
        remove_at_start = [
            "Absence de nécessité de réaliser une évaluation environnementale de la ",
            "Cadrage préalable du ",
        ]
        for start in remove_at_start:
            if item["project"].startswith(start):
                item["project"] = item["project"][len(start) :]

        item["project"] = item["project"].strip()
        item["project"] = item["project"][0].capitalize() + item["project"][1:]

        #         if not item["project"] == "Error":
        #             if item["project"].endswith("))"):
        #                 item["project"] = item["project"][:-1]
        #             if item["project"].startswith("("):
        #                 item["project"] = item["project"].lstrip("(")
        #             if "  " in item["project"]:
        #                 item["project"] = item["project"].replace("  ", " ")

        #         # Petitioner
        #         item["petitioner"] = item["petitioner"].strip()

        #         remove_at_start = ["la ", "le ", "l'", "d'", "l’", "d’", "M. le ", "M. le"]
        #         for start in remove_at_start:
        #             if item["petitioner"].startswith(start):
        #                 item["petitioner"] = item["petitioner"][len(start) :]

        #         item["petitioner"] = item["petitioner"].strip()
        #         item["petitioner"] = item["petitioner"][0].capitalize() + item["petitioner"][1:]

        #         if "et de la commune" in item["petitioner"]:
        #             item["petitioner"] = item["petitioner"].replace(
        #                 "et de la commune", "et commune"
        #             )

        #         delete_after = [" en application de", " après examen au cas par cas"]
        #         for d in delete_after:
        #             if d in item["petitioner"]:
        #                 item["petitioner"] = item["petitioner"].split(d)[0]

        #         if re.search("de[A-Z]", item["petitioner"]):
        #             item["petitioner"] = re.sub(r"de([A-Z])", r"de \1", item["petitioner"])

        #         item["petitioner"] = (
        #             item["petitioner"].replace("( ", "(").replace("  ", " ").rstrip(".,")
        #         )

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
            raise DropItem("Upload limit exceeded.")


class HandleErrorsPipeline:
    """Pass docs with errors to private"""

    def process_item(self, item, spider):

        if (
            item["project"].lower() == "error"
            or item["petitioner"].lower() == "error"
            or item["decision_date_string"].lower() == "error"
            or item["decision_date"].lower() == "error"
            or "error" in item["title"].lower()
        ):
            item["error"] = True
            item["access"] = "private"
        else:
            item["error"] = False
            item["access"] = spider.access_level

        return item


class UploadPipeline:
    """Upload document to DocumentCloud & store event data."""

    def open_spider(self, spider):


        if not spider.dry_run:
            try:
                spider.event_data = spider.load_event_data()
                spider.logger.info(
                    f"Loaded event data ({len(spider.event_data)} documents)"
                )
            except Exception as e:
                raise Exception("Error loading event data").with_traceback(
                    e.__traceback__
                )
                sys.exit(1)
        else:
            spider.event_data = None

        if spider.event_data is None:
            spider.event_data = {}

    def process_item(self, item, spider):

        if not spider.dry_run:
            try:
                spider.client.documents.upload(
                    item["source_file_url"],
                    project=spider.target_project,
                    title=item["title"],
                    description=item["project"],
                    source="www.igedd.developpement-durable.gouv.fr",
                    language="fra",
                    access=item["access"],
                    data={
                        "category": item["category"],
                        "category_local": item["category_local"],
                        "source_import": "IGEDD Scraper",
                        "source_file_url": item["source_file_url"],
                        "source_filename": item["source_filename"],
                        "source_page_url": item["source_page_url"],
                        "publication_date": item["publication_date"],
                        "publication_time": item["publication_time"],
                        "publication_datetime": item["publication_datetime"],
                        "decision_date": item["decision_date"],
                        "petitioner": item["petitioner"],
                    },
                )
            except Exception as e:
                raise Exception("Upload error").with_traceback(e.__traceback__)

            else:  # No upload error, add to event_data
                now = datetime.datetime.now().isoformat()
                spider.event_data[item["source_file_url"]] = {
                    "headers": item["headers"],
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


class MailPipeline:
    """Send scraping run report."""

    def open_spider(self, spider):
        self.items_ok = []
        self.items_with_error = []

    def process_item(self, item, spider):

        if (
            item["project"] == "ERROR"
            or item["petitioner"] == "ERROR"
            or item["decision_date_string"] == "ERROR"
            or item["decision_date"] == "ERROR"
        ):
            self.items_with_error.append(item)
        else:
            self.items_ok.append(item)

        return item

    def close_spider(self, spider):

        def print_item(item, error=False):
            item_string = f"""
            title: {item["title"]}
            project: {item["project"]}
            petitioner: {item["petitioner"]}
            category: {item["category"]}
            category_local: {item["category_local"]}
            decision_date: {item["decision_date"]}
            publication_date: {item["publication_date"]}
            source_file_url: {item["source_file_url"]}
            source_page_url: {item["source_page_url"]}
            """

            if error:
                item_string = item_string + f"\nfull_info: {item['full_info']}"

            return item_string

        subject = f"IGEDD Scraper (Errors: {len(self.items_with_error)} | New: {len(self.items_ok)} )"

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
