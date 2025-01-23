import re
from datetime import datetime, timedelta

import scrapy
from scrapy.exceptions import CloseSpider

from ..items import DocumentItem

AUTHORITY = "IGEDD"


class IGEDDSpider(scrapy.Spider):
    name = "IGEDD_spider"

    # allowed_domains = [
    #     "www.igedd.developpement-durable.gouv.fr",
    #     "webissimo.developpement-durable.gouv.fr",
    #     "webissimo-inter.e2.rie.gouv.fr",
    # ]

    start_urls = [
        "https://www.igedd.developpement-durable.gouv.fr/l-autorite-environnementale-r145.html"
    ]

    upload_limit_attained = False

    start_time = datetime.now()

    def check_time_limit(self):
        """Closes the spider automatically if it reaches a specified duration"""

        # self.logger.info(f"Checking time limit ({self.time_limit} min)")

        if self.time_limit != 0:

            limit = self.time_limit * 60
            now = datetime.now()

            if timedelta.total_seconds(now - self.start_time) > limit:
                raise CloseSpider(
                    f"Closed due to time limit ({self.time_limit} minutes)"
                )

    def check_upload_limit(self):
        """Closes the spider if the upload limit is attained."""
        if self.upload_limit_attained:
            raise CloseSpider("Closed due to max documents limit.")

    def parse(self, response):
        """Parse home page"""

        sections = response.css("#contenu .liste-rubriques > div")

        for section in sections:

            if section.css(".item-liste-rubriques-seule"):
                # rubrique seule
                link = section.css(".fr-tile__link")

                title = link.css("::text").get()
                if title == "Les saisines":

                    yield response.follow(
                        link.attrib["href"],
                        callback=self.parse_year_selection_page,
                        cb_kwargs=dict(category_local="Saisines"),
                    )

            elif section.css(".rubrique_avec_sous-rubriques"):
                # rubrique avec sous-rubriques
                title = section.css(".fr-tile__title::text").get().strip()
                subsections = section.css(".lien-sous-rubrique")

                if title == "Avis rendus":

                    current_year_subsec = subsections[0]
                    current_year_subsec_title = current_year_subsec.css("::text").get()

                    archives_subsec = subsections[1]

                    if all(
                        [str(y) in current_year_subsec_title for y in self.target_years]
                    ):
                        # All target years are in the current year link
                        follow_current = True
                        follow_archives = False

                    elif any(
                        [str(y) in current_year_subsec_title for y in self.target_years]
                    ):
                        # Some target years are in the current year link
                        follow_current = True
                        follow_archives = True
                    else:
                        # No target years are in the current year link
                        follow_current = False
                        follow_archives = True

                    if follow_current:
                        yield response.follow(
                            current_year_subsec.attrib["href"],
                            callback=self.parse_year_selection_page,
                            cb_kwargs=dict(category_local="Avis rendus"),
                        )

                    if follow_archives:
                        yield response.follow(
                            archives_subsec.attrib["href"],
                            callback=self.parse_year_selection_page,
                            cb_kwargs=dict(category_local="Avis rendus"),
                        )

                elif title == "Examen au cas par cas et autres décisions":

                    for subsec in subsections:
                        link_url = subsec.attrib["href"]
                        link_text = subsec.css("::text").get()

                        self.logger.debug(f"Following {link_text} / {link_url}")

                        yield response.follow(
                            link_url,
                            callback=self.parse_current_or_archives_page,
                            cb_kwargs=dict(category_local=link_text),
                        )

    def parse_current_or_archives_page(self, response, category_local):
        # https://www.igedd.developpement-durable.gouv.fr/decisions-de-cas-par-cas-sur-des-projets-r506.html
        # https://www.igedd.developpement-durable.gouv.fr/decisions-de-cas-par-cas-sur-des-plans-programmes-r507.html

        self.check_upload_limit()
        self.check_time_limit()

        options = response.css("#contenu .fr-tile__link")

        current_year_link = options[0]
        current_year_link_text = current_year_link.css("::text").get()

        archives_link = options[-1]
        archives_link_text = archives_link.css("::text").get()

        print(
            f"### CURRENT = '{current_year_link_text}', ARCHIVES = '{archives_link_text}'"
        )

        if all([str(y) in current_year_link_text for y in self.target_years]):
            follow_current_year = True
            follow_archives = False
        elif any([str(y) in current_year_link_text for y in self.target_years]):
            follow_current_year = True
            follow_archives = True
        else:
            follow_current_year = False
            follow_archives = True

        if follow_current_year:
            yield response.follow(
                current_year_link.attrib["href"],
                callback=self.parse_documents_page,
                cb_kwargs=dict(category_local=category_local),
            )

        if follow_archives:
            yield response.follow(
                archives_link.attrib["href"],
                callback=self.parse_year_selection_page,
                cb_kwargs=dict(category_local=category_local),
            )

    def parse_year_selection_page(self, response, category_local):

        self.check_upload_limit()
        self.check_time_limit()

        card_links = response.css("#contenu .fr-card__link")

        if len(card_links) == 1:

            card_link = card_links[0]

            yield response.follow(
                card_link.attrib["href"],
                callback=self.parse_documents_page,
                cb_kwargs=dict(category_local=category_local),
            )

        else:

            for link in card_links:

                link_text = link.css("::text").get()

                year_match = re.search("20\d\d", link_text)

                if year_match:
                    if any([str(y) == year_match.group() for y in self.target_years]):
                        self.logger.debug(f"matched '{link_text}'")
                        yield response.follow(
                            link.attrib["href"],
                            callback=self.parse_documents_page,
                            cb_kwargs=dict(category_local=category_local),
                        )

    def parse_documents_page(self, response, category_local):

        # Avis rendus: https://www.igedd.developpement-durable.gouv.fr/2024-r708.html?lang=fr
        # Décisions de cas par cas sur des plans-programmes: https://www.igedd.developpement-durable.gouv.fr/2024-en-cours-d-examen-et-decisions-rendues-r750.html?lang=fr
        # Décisions de cas par cas sur des projets: https://www.igedd.developpement-durable.gouv.fr/2024-en-cours-d-examen-et-decisions-rendues-r755.html?lang=fr
        # Les saisines: https://www.igedd.developpement-durable.gouv.fr/les-saisines-de-l-autorite-environnementale-du-a417.html?lang=fr

        # def parse_no_dossier(full_info, category_local):
        #     """Extracts dossier number from full info"""

        #     if category_local == "Avis rendus":
        #         match_no_dossier = re.search(
        #             r"(?:N°dossier Ae\xa0: |N°\xa0|N°)(.*)\n", full_info, re.IGNORECASE
        #         )
        #     elif category_local.startswith("Décisions de cas par cas"):
        #         match_no_dossier = re.search(
        #             r"N° Ae-CERFA :(.*)\n", full_info, re.IGNORECASE
        #         )

        #     if match_no_dossier:
        #         no_dossier = match_no_dossier.group(1).strip()
        #     else:
        #         no_dossier = "ERROR"

        #     return no_dossier

        # Main fuction

        self.check_upload_limit()
        self.check_time_limit()

        page_title = response.xpath("//title/text()").get().replace(" |  IGEDD", "")

        self.logger.info(f'Parsing page "{page_title}"')

        if category_local == "Avis rendus":

            page_title = response.xpath("//title/text()").get()
            page_year = re.search("20\d\d", page_title)[0]

            content_elements = response.css(
                "#contenu .contenu-article .texte-article > *"
            )

            for elem in content_elements:
                if elem.css("h2"):
                    decision_date_line = elem.css("h2::text").get()
                    decision_date_string = decision_date_line.replace("Séance du ", "")

                elif elem.css(".texteencadre-spip"):

                    encadre = elem.css(".texteencadre-spip")

                    if encadre.css("a.fr-download__link"):

                        # Extract document info and yield new request

                        full_info = "".join(
                            [
                                x
                                for x in encadre.css("::text").getall()
                                if x != "NOUVEAU"
                            ]
                        )

                        project = encadre.css(".fr-download__link ::text").get().strip()

                        # no_dossier = parse_no_dossier(full_info, category_local)

                        if "cadrage préalable" in project.lower():
                            title = f"Cadrage préalable"
                        else:
                            title = f"Avis"

                        doc_link = encadre.css("a.fr-download__link").attrib["href"]

                        doc_item = DocumentItem(
                            title=title,
                            project=project,
                            authority=AUTHORITY,
                            category_local=category_local,
                            source_file_url=response.urljoin(doc_link),
                            source_page_url=response.request.url,
                            full_info=full_info,
                            year=page_year,
                        )

                        for y in self.target_years:
                            if (
                                str(y) in decision_date_line
                                and not doc_item["source_file_url"] in self.event_data
                            ):
                                doc_item["year"] = str(y)

                                yield response.follow(
                                    doc_item["source_file_url"],
                                    method="HEAD",
                                    callback=self.parse_document_headers,
                                    cb_kwargs=dict(doc_item=doc_item),
                                )

        elif category_local.startswith("Décisions de cas par cas"):

            page_title = response.xpath("//title/text()").get()
            page_year = re.search("20\d\d", page_title)[0]

            content_elements = response.css(
                "#contenu .contenu-article .texte-article > *"
            )

            section = "?"
            for elem in content_elements:
                if elem.css(
                    "h2"
                ):  # Used to detect pending/taken decisions, not used for now
                    h2_text = elem.css("h2::text").get()

                    if "en cours" in h2_text:
                        section = "en cours"

                    elif "décisions prises" in h2_text:
                        section = "décisions prises"

                elif elem.css(".texteencadre-spip"):

                    encadre = elem.css(".texteencadre-spip")

                    full_info = "".join(encadre.css("::text").getall())

                    # no_dossier = parse_no_dossier(full_info, category_local)

                    # Project
                    project_link = encadre.css("a.spip_out::text")
                    if project_link:
                        project = project_link.get().strip()
                    else:
                        project_match = re.search(
                            "Nom et formulaire du dossier : (.*)\n", full_info
                        )
                        if project_match:
                            project = project_match.group(1).strip()
                        else:
                            project = "ERROR"

                    # links in boxes (avis, recours, lettres, etc)
                    box_links = encadre.css("a.fr-download__link")
                    for link in box_links:
                        link_url = link.attrib["href"]
                        link_text = link.css("::text").get().strip()
                        if link_text in ["OUI", "NON"]:
                            title = f"Décision ({link_text})"
                        else:
                            title = link_text.strip()

                        doc_item = DocumentItem(
                            title=title,
                            category_local=category_local,
                            authority=AUTHORITY,
                            full_info=full_info,
                            project=project,
                            source_page_url=response.request.url,
                            source_file_url=response.urljoin(link_url),
                            year=page_year,
                        )

                        if not doc_item["source_file_url"] in self.event_data:
                            yield response.follow(
                                doc_item["source_file_url"],
                                method="HEAD",
                                callback=self.parse_document_headers,
                                cb_kwargs=dict(doc_item=doc_item),
                            )

                    # simple links (formulaire, recours)
                    simple_links = encadre.css("a.spip_out")
                    if simple_links:
                        for index, link in enumerate(simple_links):
                            file_url = link.attrib["href"]
                            if index == 0:
                                title = f"Formulaire"
                            else:
                                title = link.css("::text").get().strip()

                            doc_item = DocumentItem(
                                title=title,
                                category_local=category_local,
                                authority=AUTHORITY,
                                full_info=full_info,
                                project=project,
                                source_page_url=response.request.url,
                                source_file_url=response.urljoin(file_url),
                                year=page_year,
                            )

                            if not doc_item["source_file_url"] in self.event_data:
                                yield response.follow(
                                    doc_item["source_file_url"],
                                    method="HEAD",
                                    callback=self.parse_document_headers,
                                    cb_kwargs=dict(doc_item=doc_item),
                                )

        elif category_local == "Saisines":

            download_boxes = response.css("#main .texte-article .fr-download")

            for dl_box in download_boxes:

                doc_title = "".join(
                    [
                        x.strip()
                        for x in dl_box.css("a.fr-download__link::text").getall()
                        if x.strip()
                    ]
                )
                doc_link = dl_box.css("a.fr-download__link").attrib["href"]

                preceding_p = dl_box.xpath("./preceding-sibling::p")[-1]

                project = preceding_p.css("strong").css("::text").get()

                date_string = preceding_p.css("::text")[-1].get()

                year_match = re.search("20\d\d", date_string)

                year = int(year_match.group())

                for y in self.target_years:

                    if year == y:

                        doc_item = DocumentItem(
                            title=f"Accusé de reception - {doc_title}",
                            project=project,
                            authority=AUTHORITY,
                            category_local=category_local,
                            source_file_url=response.urljoin(doc_link),
                            source_page_url=response.request.url,
                            year=str(year),
                        )

                        if not doc_item["source_file_url"] in self.event_data:
                            yield response.follow(
                                doc_item["source_file_url"],
                                method="HEAD",
                                callback=self.parse_document_headers,
                                cb_kwargs=dict(doc_item=doc_item),
                            )

    def parse_document_headers(self, response, doc_item):  # à relire
        """Gets the headers of a document to extract its publication date (Last-Modified header)."""

        self.check_upload_limit()
        self.check_time_limit()

        # Use Last-Modified header as date for the document
        # Note: this is UTC
        doc_item["headers"] = dict(response.headers.to_unicode_dict())
        last_modified = response.headers.get("Last-Modified").decode("utf-8")

        doc_item["publication_lastmodified"] = last_modified

        yield doc_item
