import logging
import pathlib
import pytest
import requests
from kjoretoy import last_opp_kjoretoy, prepp_kjoretoy
from dotenv import load_dotenv
import os
logging.basicConfig(level=logging.DEBUG)

THIS_FOLDER = pathlib.Path(__file__).parent

load_dotenv(dotenv_path=THIS_FOLDER / "test.env")
connstr = os.environ.get("CONN")
TESTDATA = THIS_FOLDER / "testdata"

URL = "http://127.0.0.1:8000"


@pytest.fixture(scope="session")
def db():
    df = prepp_kjoretoy(TESTDATA / "kjoretoyinfo_2022_jan.parquet")
    last_opp_kjoretoy(
        connstr,
        df
    )


def test_kjoretoy_regdato(db):
    kjoretoy_endpoint = URL + "/regdato/20220101"
    resp = requests.get(kjoretoy_endpoint)
    svar = resp.json()

    forventet = [
        {
            "farge": "Rød (også burgunder)",
            "modell": "ADVENTURE STD 600ACE",
            "merke": "LYNX",
            "elbil": False
        },
        {
            "farge": "Svart (også blåsvart, grafitt mørk, gråsort, koksgrå mørk, koksgrå mørk metallic)",
            "modell": "2008",
            "merke": "PEUGEOT",
            "elbil": True
        }
    ]

    # Vi skal sortere lister bestående av dict, og da må vi angi manuelt hvordan disse skal sorteres med en funksjon.
    sorterer = lambda x: x["farge"] + x["modell"] + x["merke"]

    # Vi sorterer de to listene
    forventet.sort(key=sorterer)
    svar.sort(key=sorterer)

    assert svar == forventet

def test_kjoretoy_2(db):
    kjoretoy_endpoint = URL + "/regdato/20220102"
    resp = requests.get(kjoretoy_endpoint)
    svar = resp.json()

    forventet = [
        {
            "farge": "Grå",
            "modell": "FH",
            "merke": "VOLVO",
            "elbil": False
        }
    ]

    sorterer = lambda x: x["farge"] + x["modell"] + x["merke"]

    forventet.sort(key=sorterer)
    svar.sort(key=sorterer)

    assert svar == forventet

def test_pkk_dato(db):
    pkk_endpoint = URL + "/pkkdato/20241229" #Valgte denne datoen da den gir 3 forskjellige biler
    resp = requests.get(pkk_endpoint)
    svar = resp.json()

    forventet = [
        {
            "regdato": "2022-01-27",
            "modell": "Kona",
            "merke": "HYUNDAI",
            "farge": "Blå",
            "elbil": True
        },
        {
            "regdato": "2022-01-04",
            "modell": "ENYAQ 80",
            "merke": "SKODA",
            "farge": "Grå",
            "elbil": True
        },
        {
            "regdato": "2022-01-10",
            "modell": "Nissan Leaf 62kWh",
            "merke": "NISSAN",
            "farge": "Hvit (også antikkhvit, offwhite)",
            "elbil": True
        }
    ]

    sorterer = lambda x: x["regdato"] + x["modell"] + x["merke"] + x["farge"]

    forventet.sort(key=sorterer)
    svar.sort(key=sorterer)

    assert svar == forventet
