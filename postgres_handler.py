import json
import psycopg2
from psycopg2.extras import RealDictCursor

class Db:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            database="sport",
            user="robzeeman",
            password="bonzo"
        )
        self.item = []

    def version(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        cur.close()
        return {"version": db_version}

    def detail(self, id):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM vereniging_vereniging WHERE id = " + id)
        record = cur.fetchone()
        self.single_field(record, "naam", "Naam")
        self.single_field(record, "opm_naam", "Opmerkingen naam")
        self.single_field(record, "plaats", "Plaats")
        self.get_provincie(record["id"])
        self.single_field(record, "doelstelling", "Doelstelling")
        self.single_field(record, "levensbeschouwing", "Levensbeschouwing")
        self.date_fields(record, "begindatum", record["begindatum_soort"], "Begindatum")
        self.date_fields(record, "einddatum", record["einddatum_soort"], "Einddatum")
        self.get_range(record["id"])
        self.get_landelijk(record["id"])
        self.get_lokaal(record["id"])
        self.get_day(record["id"])
        self.get_relations(record["id"])
        self.single_field(record, "kb", "Koninklijk besluit")
        self.single_field(record, "oprichters", "Oprichters")
        self.single_field(record, "bestuursleden", "Bestuursleden")
        self.single_field(record, "beschermheren", "Beschermheren")
        self.single_field(record, "verantwoording_gegevens", "Verantwoording gegevens")
        self.single_field(record, "opmerkingen", "Opmerkingen")
        cur.close()
        retVal = self.item.copy()
        self.item = []
        return retVal

    def single_field(self, item, key, label):
        if (item[key] != None and item[key] != ""):
            self.item.append({"field": key, "value": item[key], "label": label})
        return

    def date_fields(self, item, key, typeDate, label):
        if (item[key] != None and str(item[key]) != ""):
            self.item.append({"field": key, "value": str(item[key]) + " (" + typeDate + ")", "label": label})
        return

    def get_provincie(self, id):
        retList = []
        list = self.get_data("SELECT provincie FROM vereniging_provincie WHERE vereniging =" + str(id) + " ORDER BY provincie")
        for item in list:
            retList.append(item["provincie"].title())
        if retList:
            self.item.append({"field": "provincie", "value": ", ".join(item for item in retList), "label": "Provincie"})
        return

    def get_lokaal(self, id):
        retList = []
        list = self.get_data("SELECT naam, beginjaar, eindjaar FROM \"vereniging_Regionale_bond\" INNER JOIN \"vereniging_regionale_bond\" ON public.\"vereniging_Regionale_bond\".regionale_bond = \"vereniging_regionale_bond\".id and \"vereniging_regionale_bond\".vereniging =" + str(id))
        for item in list:
            retList.append(self.show_bond(item))
        if retList:
            self.item.append({"field": "regionale_bond", "value": "\n\n".join(item for item in retList), "label": "Regionale bond"})
        return

    def get_landelijk(self, id):
        retList = []
        list = self.get_data("SELECT naam, beginjaar, eindjaar FROM \"vereniging_Landelijke_bond\" INNER JOIN \"vereniging_landelijke_bond\" ON public.\"vereniging_Landelijke_bond\".landelijke_bond = \"vereniging_landelijke_bond\".id and vereniging_landelijke_bond.vereniging = " + str(id))
        for item in list:
            retList.append(self.show_bond(item))
        if retList:
            self.item.append({"field": "landelijke_bond", "value": "\n\n".join(item for item in retList), "label": "Landelijke bond"})
        return

    def get_relations(self, id):
        retList = []
        list = self.get_data("SELECT instelling, type_relatie FROM vereniging_relatie INNER JOIN vereniging_relaties ON vereniging_relatie.relaties = vereniging_relaties.id and vereniging_relatie.type_relatie IS NOT NULL and vereniging_relaties.vereniging = " + str(id))
        print(list)
        for item in list:
            print(self.show_relation(item))
            retList.append(self.show_relation(item))
        if retList:
            self.item.append({"relaties": "landelijke_bond", "value": "\n\n".join(item for item in retList), "label": "Relaties"})
        return

    def get_day(self, id):
        values = {"alleen_zaterdag": "Alleen zaterdag", "ook_zaterdag": "Ook zaterdag"}
        list = self.get_data("SELECT dag FROM vereniging_speeldag WHERE vereniging = " + str(id))
        if list and list[0]["dag"]:
            self.item.append({"field": "speeldag", "value": values[list[0]["dag"]], "label": "Speeldag"})

    def get_range(self, id):
        retList = []
        list = self.get_data("SELECT werkingsgebied FROM vereniging_werkingsgebied WHERE werkingsgebied is not null and vereniging =" + str(id))
        for item in list:
            retList.append(item["werkingsgebied"].title())
        if retList:
            self.item.append({"field": "werkingsgebied", "value": ", ".join(item for item in retList), "label": "Werkingsgebied"})

    def get_data(self, sql):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        records = cur.fetchall()
        cur.close()
        return records

    def show_bond(self, item):
        retString = item["naam"]
        if (item["beginjaar"] and item["eindjaar"]):
            retString = retString + " (" + str(item["beginjaar"]) + " - " + str(item["eindjaar"]) + ")"
        else:
            if item["beginjaar"]:
                retString = retString + "(" + str(item["beginjaar"]) + ")"
            else:
                if item["beginjaar"]:
                    retString = retString + "(" + str(item["beginjaar"]) + ")"
        return retString

    def show_relation(self, item):
        return self.transform_rel_type(item["type_relatie"]) + "\n" + item["instelling"]

    def transform_rel_type(self, rel_str):
        return rel_str.replace("_", " ").replace("{", "").replace("}", ":").capitalize()





