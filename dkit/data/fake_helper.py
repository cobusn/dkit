"""
canned fake data for testing purpose
"""
import random

from faker import Factory

from faker.providers import BaseProvider
from . import helpers


class DocumentProvider(BaseProvider):

    list_of_documents = [
        "minutes", "operations report", "manco report",
        "capacity plan", "cost overview", "turnaround plan",
        "sales plan", "cost strategy", "ISO 9000 Update",
        "monthly report", "daily update", "newsletter",
        "equipment inventory", "ops meeting minutes",
        "strategy document", "mission statement", "vision",
        "personal vision", "education plan", "annual budget",
        "five year sales plan",
    ]

    def document(self):
        return self.random_element(self.list_of_documents)


class ApplicationProvider(BaseProvider):

    """
    Appliction provider (e.g. license, bank account)
    """

    list_of_applications = [
        "drivers license", "tv license", "passport", "id document",
        "security clearance", "overdraft", "credit card", "credit extension",
        "loan", "bank account", "university degreee",
    ]

    def application(self):
        return self.random_element(self.list_of_applications)


class TaskProvider(BaseProvider):

    verbs = [
        "Call", "Update", "Create", "Book meeting with", "Document",
        "Apply for", "Refine"
    ]

    def __init__(self, generator):
        super().__init__(generator)
        self.fake = Factory.create()
        self.fake.add_provider(DocumentProvider)
        self.fake.add_provider(ApplicationProvider)

        self.nouns = {
            "Call": self.fake.name,
            "Book meeting with": self.fake.name,
            "Document": self.fake.document,
            "Update": self.fake.document,
            "Create": self.fake.document,
            "Apply for": self.fake.application,
            "Refine": self.fake.document,
        }

    def task(self):
        verb = self.random_element(self.verbs)
        noun = self.nouns[verb]()
        return "{} {}".format(verb, noun)


def persons(n=1000, split=0.5):
    """
    generate fake persons.

    The following fields are available:

        * first_name
        * last_name
        * job
        * city
        * country
        * address
        * birthday

    >>> l = list(persons(10))
    >>> len(l)
    10

    :param n: number of persons to generate
    :param split: split between male and female. 0.9 means 90% male
    """
    fake = Factory.create()
    i = 0

    while i < n:
        retval = {
            "last_name": fake.last_name(),
            "job": fake.job(),
            "birthday": fake.date_time_between(start_date="-60y", end_date="-20y")
        }
        if random.uniform(0.0, 1.0) < split:
            retval["first_name"] = fake.first_name_male()
            retval["gender"] = "male"
        else:
            retval["first_name"] = fake.first_name_female()
            retval["gender"] = "female"
        i += 1
        yield retval


def za_id_number(person):
    """
    Generate a South African ID number based on person

    the person records require the following fields

    .. code-block:: python

       {
            "birthday": datetime,
            "gender": "male|female"
       }

    """
    a = person["birthday"].strftime("%y%m%d")
    random_seq = random.randint(0, 4999)
    if person["gender"] == "female":
        b = "{:04d}".format(random_seq)
    else:
        b = "{:04d}".format(random_seq + 5000)
    c = "08"
    abc = a + b + c
    z = str(helpers.luhn_hash(abc))
    return abc + z
