import gzip
import orjson
from pprint import pprint
from ftmstore import get_dataset
from followthemoney import model

SCHEME_PROPS = {
    "Not a valid Org-Id scheme, provided for backwards compatibility": "registrationNumber",
    "DK Centrale Virksomhedsregister": "registrationNumber",
    "Danish Central Business Register": "registrationNumber",
    "CM EITI 2013-2015 beneficial ownership pilot": "alias",
    "GB EITI 2013-2015 beneficial ownership pilot": "alias",
    "ZM EITI 2013-2015 beneficial ownership pilot": "alias",
    "ZM EITI 2013-2015 beneficial ownership pilot": "alias",
    "GH EITI 2013-2015 beneficial ownership pilot": "alias",
    "HN EITI 2013-2015 beneficial ownership pilot": "alias",
    "ID EITI 2013-2015 beneficial ownership pilot": "alias",
    "BF EITI 2013-2015 beneficial ownership pilot": "alias",
    "MR EITI 2013-2015 beneficial ownership pilot": "alias",
    "CD EITI 2013-2015 beneficial ownership pilot": "alias",
    "TT EITI 2013-2015 beneficial ownership pilot": "alias",
    "TG EITI 2013-2015 beneficial ownership pilot": "alias",
    "TZ EITI 2013-2015 beneficial ownership pilot": "alias",
    "LR EITI 2013-2015 beneficial ownership pilot": "alias",
    "SC EITI 2013-2015 beneficial ownership pilot": "alias",
    "NG EITI 2013-2015 beneficial ownership pilot": "alias",
    "MG EITI 2013-2015 beneficial ownership pilot": "alias",
    "EITI Structured Data - Côte d'Ivoire": "alias",
    "UA Edinyy Derzhavnyj Reestr": "registrationNumber",
    "United State Register": "registrationNumber",
    "Ministry of Justice Business Register": "registrationNumber",
    "SK Register Partnerov Verejného Sektora": "registrationNumber",
    "GB Persons Of Significant Control Register": None,
    "GB Persons Of Significant Control Register - Registration numbers": "registrationNumber",
    "OpenOwnership Register": "sourceUrl",
    "OpenCorporates": "opencorporatesUrl",
    "Companies House": "registrationNumber",
}


def parse_statement(bulk, data, index):
    statement_type = data.pop("statementType")
    statement_id = data.pop("statementID")
    countries = set()

    if statement_type == "personStatement":
        person_type = data.pop("personType")
        if person_type in ("unknownPerson", "anonymousPerson"):
            return

        assert person_type == "knownPerson", (person_type, data)
        proxy = model.make_entity("Person")
        proxy.add("birthDate", data.pop("birthDate", None))
        for name in data.pop("names", []):
            proxy.add("name", name.pop("fullName"))
            # print(name)

        for nat in data.pop("nationalities", []):
            countries.add(nat.pop("code"))
            proxy.add("nationality", nat.pop("name"))

    elif statement_type == "entityStatement":
        entity_type = data.pop("entityType")
        proxy = model.make_entity("LegalEntity")
        proxy.add("name", data.pop("name", None))
        proxy.add("incorporationDate", data.pop("foundingDate", None))
        proxy.add("dissolutionDate", data.pop("dissolutionDate", None))

        juris = data.pop("incorporatedInJurisdiction", {})
        juris_code = juris.pop("code", juris.pop("name", None))
        if len(juris):
            pprint(juris)
        countries.add(juris_code)
        proxy.add("jurisdiction", juris_code)

    elif statement_type == "ownershipOrControlStatement":
        proxy = model.make_entity("Ownership")
        interested_party = data.pop("interestedParty", {})
        proxy.add("owner", interested_party.pop("describedByPersonStatement", None))
        proxy.add("owner", interested_party.pop("describedByEntityStatement", None))
        subject = data.pop("subject", {})
        proxy.add("asset", subject.pop("describedByEntityStatement", None))
        proxy.add("date", data.pop("statementDate", None))

        source = data.pop("source", {})
        proxy.add("publisher", source.pop("description", None))
        proxy.add("publisherUrl", source.pop("url", None))
        proxy.add("retrievedAt", source.pop("retrievedAt", None))

        for inter in data.pop("interests", []):
            proxy.add("role", inter.pop("type", None))
            proxy.add("summary", inter.pop("details", None))
            proxy.add("startDate", inter.pop("startDate", None))
            proxy.add("endDate", inter.pop("endDate", None))

        if len(data):
            pprint(data)

    else:
        print("UNKNOWN STATEMENT TYPE", statement_type)

    proxy.id = statement_id

    for addr in data.pop("addresses", []):
        proxy.add("address", addr.pop("address"))
        country = addr.pop("country", None)
        if country not in countries:
            countries.add(country)
            proxy.add("country", country)

    for ident in data.pop("identifiers", []):
        scheme = ident.pop("schemeName")
        value = ident.pop("uri", ident.pop("id", None))
        if scheme not in SCHEME_PROPS:
            print("UNKNOWN SCHEME", repr(scheme), value)
            continue
        if value is None:
            print("WEIRD IDENT", ident)
        prop = SCHEME_PROPS[scheme]
        if prop is not None:
            proxy.add(prop, value)

    if len(data):
        pprint({"type": statement_type, "data": data})
    bulk.put(proxy, fragment=index)


def parse_file(file_name):
    dataset = get_dataset("bods-registry")
    dataset.delete()
    bulk = dataset.bulk(size=5000)
    with gzip.open(file_name) as fh:
        index = 0
        while line := fh.readline():
            data = orjson.loads(line)
            parse_statement(bulk, data, index)
            index += 1
            if index > 0 and index % 10000 == 0:
                print("statements: ", index)
    bulk.flush()


if __name__ == "__main__":
    fn = "data/statements.latest.jsonl.gz"
    parse_file(fn)
