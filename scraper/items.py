"""Models for the scraped items."""

from scrapy.item import Item, Field


class DocumentItem(Item):
    """A document that will be uploaded to DocumentCloud."""

    title = Field()
    project = Field()
    # region = Field()
    source = Field()
    access = Field()

    category = Field()
    category_local = Field()

    source_file_url = Field()
    source_filename = Field()

    source_page_url = Field()

    publication_date = Field()
    publication_time = Field()
    publication_datetime = Field()

    publication_timestamp = Field()
    # publication_year = Field()
    # publication_month = Field()
    # publication_day = Field()
    publication_lastmodified = Field()

    full_info = Field()

    decision_date_string = Field()
    decision_date_line = Field()
    decision_date = Field()
    # decision_year = Field()
    # decision_month = Field()
    # decision_day = Field()

    petitioner = Field()

    headers = Field()

    error = Field()
