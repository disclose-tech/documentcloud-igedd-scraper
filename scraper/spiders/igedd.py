import datetime
import re

import scrapy
from scrapy.exceptions import CloseSpider

from ..items import DocumentItem


class IGEDDSpider(scrapy.Spider):
    name = "IGEDD_spider"

    allowed_domains = ["www.igedd.developpement-durable.gouv.fr"]

    start_urls = [
        "https://www.igedd.developpement-durable.gouv.fr/l-autorite-environnementale-r145.html"
    ]

    upload_limit_attained = False

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
                        cb_kwargs=dict(category_local=title),
                    )

            elif section.css(".rubrique_avec_sous-rubriques"):
                # rubrique avec sous-rubriques
                title = section.css(".fr-tile__title::text").get().strip()
                subsections = section.css(".lien-sous-rubrique")

                if title == "Avis rendus":

                    current_year_subsec = subsections[0]
                    current_year = int(current_year_subsec.css("::text").get())

                    archives_subsec = subsections[1]

                    if current_year == self.target_year:
                        # print("---> Target year matches current year")
                        yield response.follow(
                            current_year_subsec.attrib["href"],
                            callback=self.parse_year_selection_page,
                            cb_kwargs=dict(category_local="Avis rendus"),
                        )

                    else:
                        # print("---> Target year does NOT matches current year")
                        yield response.follow(
                            archives_subsec.attrib["href"],
                            callback=self.parse_year_selection_page,
                            cb_kwargs=dict(category_local="Avis rendus"),
                        )

                        # print(link)

                elif title == "Examen au cas par cas et autres décisions":

                    for subsec in subsections:
                        link_url = subsec.attrib["href"]
                        link_text = subsec.css("::text").get()
                        yield response.follow(
                            link_url,
                            callback=self.parse_current_or_archives_page,
                            cb_kwargs=dict(category_local=link_text),
                        )

    def parse_current_or_archives_page(self, response, category_local):
        # https://www.igedd.developpement-durable.gouv.fr/decisions-de-cas-par-cas-sur-des-projets-r506.html
        # https://www.igedd.developpement-durable.gouv.fr/decisions-de-cas-par-cas-sur-des-plans-programmes-r507.html

        options = response.css("#contenu .fr-tile__link")

        year_link_found = False
        archive_link = None

        for opt in options:
            link_text = opt.css("::text").get()
            if str(self.target_year) in link_text:  # following curent year link
                year_link_found = True
                yield response.follow(
                    opt.attrib["href"],
                    callback=self.parse_year_selection_page,
                    cb_kwargs=dict(category_local=category_local),
                )
            elif "ARCHIVES" in link_text:
                archive_link = opt

        if not year_link_found:  # Following archive link
            yield response.follow(
                archive_link.attrib["href"],
                callback=self.parse_year_selection_page,
                cb_kwargs=dict(category_local=category_local),
            )

    def parse_year_selection_page(self, response, category_local):

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
                    if int(year_match.group()) == self.target_year:
                        print(f" -> {category_local}: following {link_text}")
                        response.follow(
                            link.attrib["href"],
                            callback=self.parse_documents_page,
                            cb_kwargs=dict(category_local=category_local),
                        )

    def parse_documents_page(self, response, category_local):

        # Avis rendus (ok): https://www.igedd.developpement-durable.gouv.fr/2024-r708.html?lang=fr
        # Décisions de cas par cas sur des plans-programmes: https://www.igedd.developpement-durable.gouv.fr/2024-en-cours-d-examen-et-decisions-rendues-r750.html?lang=fr
        # Décisions de cas par cas sur des projets: https://www.igedd.developpement-durable.gouv.fr/2024-en-cours-d-examen-et-decisions-rendues-r755.html?lang=fr
        # Les saisines: https://www.igedd.developpement-durable.gouv.fr/les-saisines-de-l-autorite-environnementale-du-a417.html?lang=fr

        def parse_no_dossier(full_info, category_local):
            """Extracts dossier number from full info"""

            if category_local == "Avis rendus":
                match_no_dossier = re.search(
                    r"(?:N°dossier Ae\xa0: |N°\xa0)(.*)\n", full_info, re.IGNORECASE
                )
            elif category_local.startswith("Décisions de cas par cas"):
                match_no_dossier = re.search(
                    r"N° Ae-CERFA : (.*)\n", full_info, re.IGNORECASE
                )

            if match_no_dossier:
                no_dossier = match_no_dossier.group(1)
            else:
                no_dossier = "ERROR"

            return no_dossier

        # Main fuction

        if category_local == "Avis rendus":

            content_elements = response.css(
                "#contenu .contenu-article .texte-article > *"
            )
            for elem in content_elements:
                if elem.css("h2"):
                    decision_date_line = elem.css("h2::text").get()

                    decision_date_string = decision_date_line.replace("Séance du ", "")

                    # Extract date from the title "Séance du"
                    # date =

                elif elem.css(".texteencadre-spip"):

                    # print(elem.css(".texteencadre-spip ::text").getall())
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

                        no_dossier = parse_no_dossier(full_info, category_local)

                        doc_link = encadre.css("a.fr-download__link").attrib["href"]

                        doc_item = DocumentItem(
                            title=f"Avis {no_dossier}",
                            project=project,
                            # region=region,
                            category_local=category_local,
                            source_file_url=response.urljoin(doc_link),
                            source_page_url=response.request.url,
                            full_info=full_info,
                            # decision_date_line=date,
                            decision_date_string=decision_date_string,
                            petitioner="",
                        )

                    # TODO: Check year ??
                    if not doc_item["source_file_url"] in self.event_data:
                        yield response.follow(
                            doc_link,
                            method="HEAD",
                            callback=self.parse_document_headers,
                            cb_kwargs=dict(doc_item=doc_item),
                        )

        elif category_local.startswith("Décisions de cas par cas"):

            content_elements = response.css(
                "#contenu .contenu-article .texte-article > *"
            )

            # print(str(len(content_elements)) + " " + response.request.url)

            section = "?"
            for elem in content_elements:
                if elem.css("h2"):
                    h2_text = elem.css("h2::text").get()

                    if "en cours" in h2_text:
                        section = "en cours"

                    elif "décisions prises" in h2_text:
                        section = "décisions prises"

                elif elem.css(".texteencadre-spip"):

                    if section == "décisions prises":

                        encadre = elem.css(".texteencadre-spip")

                        full_info = "".join(encadre.css("::text").getall())

                        no_dossier = parse_no_dossier(full_info, category_local)

                        # Petitioner
                        match_petitioner = re.search(
                            "Pétitionnaire ou maître d’ouvrage\xa0: ?(.*)\n", full_info
                        )
                        if match_petitioner:
                            petitioner = match_petitioner.group(1).strip()
                        else:
                            petitioner = "ERROR"

                        # Project
                        project = encadre.css("a.spip_out::text").get().strip()

                        # decision_date
                        match_decision_date = re.search(
                            r"Décision du (.*) \(\*\)", full_info
                        )
                        if match_decision_date:
                            decision_date = match_decision_date.group(1).strip()
                        else:
                            decision_date = "ERROR"

                        # decision_url
                        decision_link = encadre.css("a.fr-download__link")

                        if decision_link:

                            decision_file_url = decision_link.attrib["href"]
                            decision_doc_item = DocumentItem(
                                title=f"Décision {no_dossier}",
                                category_local=category_local,
                                full_info=full_info,
                                project=project,
                                petitioner=petitioner,
                                source_page_url=response.request.url,
                                decision_date_string=decision_date,
                                source_file_url=response.urljoin(decision_file_url),
                            )
                            if (
                                not decision_doc_item["source_file_url"]
                                in self.event_data
                            ):
                                yield response.follow(
                                    decision_file_url,
                                    callback=self.parse_document_headers,
                                    cb_kwargs=dict(
                                        doc_item=decision_doc_item,
                                    ),
                                )

                        # formulaire_url
                        formulaire_link = encadre.css("a.spip_out")
                        if formulaire_link:
                            formulaire_file_url = formulaire_link.attrib["href"]

                            formulaire_doc_item = DocumentItem(
                                title=f"Formulaire {no_dossier}",
                                category_local=category_local,
                                full_info=full_info,
                                project=project,
                                petitioner=petitioner,
                                source_page_url=response.request.url,
                                decision_date_string=decision_date,
                                source_file_url=response.urljoin(formulaire_file_url),
                            )
                            if (
                                not formulaire_doc_item["source_file_url"]
                                in self.event_data
                            ):
                                yield response.follow(
                                    decision_file_url,
                                    callback=self.parse_document_headers,
                                    cb_kwargs=dict(
                                        doc_item=formulaire_doc_item,
                                    ),
                                )

        # else:
        #     print("did not handle category:" + category_local)

    def parse_document_headers(self, response, doc_item):  # à relire
        """Gets the headers of a document to extract its publication date (Last-Modified header)."""

        self.check_upload_limit()

        # Use Last-Modified header as date for the document
        # Note: this is UTC
        doc_item["headers"] = dict(response.headers.to_unicode_dict())
        last_modified = response.headers.get("Last-Modified").decode("utf-8")

        doc_item["publication_lastmodified"] = last_modified

        # dt = datetime.datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")

        yield doc_item
