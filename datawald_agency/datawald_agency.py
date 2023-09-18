#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

from .datawald_abstract import Abstract
from cerberus import Validator, TypeDefinition
from decimal import Decimal

Validator.types_mapping["decimal"] = TypeDefinition("decimal", (Decimal,), ())


class Agency(Abstract):
    def __init__(self, logger, datawald=None):
        self.logger = logger
        self.datawald = datawald
        self._tx_type = {
            "transaction": [
                "order",
                "invoice",
                "purchaseorder",
                "itemreceipt",
                "itemfulfillment",
                "opportunity",
                "quote",
                "rma",
                "billcredit",
                "payment",
                "inventoryadjustment",
                "creditmemo",
                "inventorytransfer",
            ],
            "asset": ["product", "inventory", "inventorylot", "pricelevel"],
            "person": ["customer", "vendor", "company", "contact"],
        }

    @property
    def tx_type(self):
        return self._tx_type

    @tx_type.setter
    def tx_type(self, tx_type):
        self._tx_type = tx_type

    ## Insert update entities to target.
    def insert_update_entities_to_target(self, **kwargs):
        tx_type = kwargs["entities"][0].get("tx_type_src_id").split("-")[0]
        if tx_type in self.tx_type["transaction"]:
            tx_entity_tgt = self.tx_transaction_tgt
            tx_entity_tgt_ext = self.tx_transaction_tgt_ext
            insert_update_entities = self.insert_update_transactions
        elif tx_type in self.tx_type["asset"]:
            tx_entity_tgt = self.tx_asset_tgt
            tx_entity_tgt_ext = self.tx_asset_tgt_ext
            insert_update_entities = self.insert_update_assets
        elif tx_type in self.tx_type["person"]:
            tx_entity_tgt = self.tx_person_tgt
            tx_entity_tgt_ext = self.tx_person_tgt_ext
            insert_update_entities = self.insert_update_persons
        else:
            raise Exception(
                f"The tx_type({tx_type}) is not supported for 'insert_entities_to_target'!!!"
            )

        kwargs.update(
            {
                "tx_entity_tgt": tx_entity_tgt,
                "tx_entity_tgt_ext": tx_entity_tgt_ext,
                "insert_update_entities": insert_update_entities,
            }
        )
        return super().insert_update_entities_to_target(**kwargs)

    def tx_transaction_tgt(self, transaction):
        return {}

    def tx_transaction_tgt_ext(self, new_transaction, transaction):
        pass

    def insert_update_transactions(self, transactions):
        return None

    def tx_asset_tgt(self, asset):
        return {}

    def tx_asset_tgt_ext(self, new_asset, asset):
        pass

    def insert_update_assets(self, assets):
        return None

    def tx_person_tgt(self, person):
        return {}

    def tx_person_tgt_ext(self, new_person, person):
        pass

    def insert_update_persons(self, persons):
        return None

    ## Retrieve entities from source.
    def retrieve_entities_from_source(self, **kwargs):
        if kwargs.get("tx_type") in self.tx_type["transaction"]:
            get_entities_total = self.get_transactions_total
            tx_entities_src = self.tx_transactions_src
            tx_entities_src_ext = self.tx_transactions_src_ext
            validate_data = self.validate_transaction_data
        elif kwargs.get("tx_type") in self.tx_type["asset"]:
            get_entities_total = self.get_assets_total
            tx_entities_src = self.tx_assets_src
            tx_entities_src_ext = self.tx_assets_src_ext
            validate_data = self.validate_asset_data
        elif kwargs.get("tx_type") in self.tx_type["person"]:
            get_entities_total = self.get_persons_total
            tx_entities_src = self.tx_persons_src
            tx_entities_src_ext = self.tx_persons_src_ext
            validate_data = self.validate_person_data
        else:
            raise Exception(
                f"The tx_type({kwargs.get('tx_type')}) is not supported for 'retrieve_entities_from_source'!!!"
            )

        kwargs.update(
            {
                "get_entities_total": get_entities_total,
                "tx_entities_src": tx_entities_src,
                "tx_entities_src_ext": tx_entities_src_ext,
                "validate_data": validate_data,
            }
        )
        return super().retrieve_entities_from_source(**kwargs)

    def get_transactions_total(self, **kwargs):
        return 0

    def tx_transactions_src(self, **kwargs):
        return []

    def tx_transactions_src_ext(self, transactions, **kwargs):
        pass

    def validate_transaction_data(self, transaction, **kwargs):
        pass

    def get_assets_total(self, **kwargs):
        return 0

    def tx_assets_src(self, **kwargs):
        return []

    def tx_assets_src_ext(self, assets, **kwargs):
        pass

    def get_product_metadatas(self, **kwargs):
        headers = self.datawald.get_product_metadatas(
            **{"target_source": f"{kwargs.get('target')}-{kwargs.get('source')}"}
        )
        metadatas = {
            metadata["dest"]: {
                "src": metadata["src"],
                "funct": metadata["funct"],
                "type": "attribute",
            }
            for metadata in [header["metadata"] for header in headers]
        }
        return metadatas

    def validate_asset_data(self, asset, **kwargs):
        if kwargs.get("tx_type") == "product":
            self.validate_product_data(asset, **kwargs)

        pass

    def validate_product_data(self, asset, **kwargs):
        headers = self.datawald.get_product_metadatas(
            **{"target_source": f"{kwargs.get('target')}-{kwargs.get('source')}"}
        )
        schema = {
            list(header["metadata"]["schema"].keys())[0]: list(
                header["metadata"]["schema"].values()
            )[0]
            for header in headers
            if header["metadata"]["schema"] is not None
        }
        product_validate = Validator()
        # product_validate.allow_unknown = True
        if not product_validate.validate(asset["data"], schema):
            raise Exception(product_validate.errors)

    def get_persons_total(self, **kwargs):
        return 0

    def tx_persons_src(self, **kwargs):
        return []

    def tx_persons_src_ext(self, persons, **kwargs):
        pass

    def validate_person_data(self, person, **kwargs):
        if kwargs.get("tx_type") == "customer":
            self.validate_customer_data(person, **kwargs)
        elif kwargs.get("tx_type") == "vendor":
            self.validate_vendor_data(person, **kwargs)
        elif kwargs.get("tx_type") == "company":
            self.validate_company_data(person, **kwargs)
        elif kwargs.get("tx_type") == "contact":
            self.validate_contact_data(person, **kwargs)
        else:
            pass

    def validate_customer_data(self, person, **kwargs):
        pass

    def validate_vendor_data(self, person, **kwargs):
        pass

    def validate_company_data(self, person, **kwargs):
        pass

    def validate_contact_data(self, person, **kwargs):
        pass
