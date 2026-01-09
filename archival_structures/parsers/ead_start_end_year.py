import json

from lxml import etree
import pandas as pd


def extract_dates(input_file: str, output_base_name: str):
    data = []
    data_json = dict()

    tree = etree.parse(input_file)

    # Find all inventory numbers in the EAD
    # <c level="file">
    inventory_elements = tree.xpath("//c[@level='file']")

    for c in inventory_elements:
        name = c.find("did/unitid[@type='ABS']")
        title = c.find("did/unittitle")
        handle = c.find("did/unitid[@type='handle']")
        date = c.find("did/unitdate")
        date_alt = c.find("did/unittitle/unitdate")

        if date is not None:
            date_text = date.text
            date_iso = date.attrib.get("normal", "")
        elif date_alt is not None:
            date_text = date_alt.text
            date_iso = date_alt.attrib.get("normal", "")
        else:
            date_text = ""
            date_iso = ""

        if name is not None and handle is not None:
            # print(name.text, handle.text, date_text, date_iso)

            year_begin, year_end = get_begin_end_year(date_iso)

            d = {
                "inventory_number": name.text,
                "title": title.text.strip(),
                "handle": handle.text,
                "date_text": date_text,
                "date_iso": date_iso,
                "year_begin": year_begin,
                "year_end": year_end,
            }

            data.append(d)
            data_json[name.text] = d

    df = pd.DataFrame(data)
    df.to_csv(f"{output_base_name}.csv", sep="\t", header=True, index=False)
    with open(f"{output_base_name}.json", "w") as f:
        json.dump(data_json, f, indent=4)


def get_begin_end_year(date):
    if "/" in date:
        date_begin, date_end = date.split("/")

        return date_begin[:4], date_end[:4]
    elif len(date) == 4:
        return date, date
    elif len(date) == 7 or len(date) == 10:
        return date[:4], date[:4]
    elif len(date) == 0:
        return "", ""
    else:
        return "", ""


if __name__ == "__main__":
    ead_file = "../metadata/EAD/1.04.02.xml"
    ead_output_base = '../metadata/inv_dates-1.04.02'
    extract_dates(ead_file, ead_output_base)
