#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import traceback, random, re
from datetime import datetime
from decimal import Decimal
from pytz import timezone
from time import sleep
from silvaengine_utility import Utility


class Abstract(object):
    def insert_update_entities_to_target(self, **kwargs):
        entities = kwargs.get("entities")
        tx_entity_tgt = kwargs.pop("tx_entity_tgt")
        tx_entity_tgt_ext = kwargs.pop("tx_entity_tgt_ext")

        insert_update_entities = kwargs.pop("insert_update_entities")

        new_entities = list(
            map(
                lambda entity: self.tx_entity(tx_entity_tgt, tx_entity_tgt_ext, entity),
                entities,
            )
        )
        target_entities = insert_update_entities(
            list(filter(lambda x: x.get("tx_status") == "N", new_entities))
        )

        entity_statuses = [
            {
                "source": entity["source"],
                "tx_type_src_id": entity["tx_type_src_id"],
                "target": entity["target"],
                "tgt_id": entity["tgt_id"],
                "tx_status": entity["tx_status"],
                "tx_note": entity["tx_note"],
                "updated_at": entity["updated_at"],
            }
            for entity in target_entities
        ]

        for entity_status in entity_statuses:
            try:
                self.datawald.update_tx_staging(**entity_status)
            except Exception:
                log = traceback.format_exc()
                self.logger.exception(log)
                raise
        return Utility.json_dumps(entity_statuses)

    def tx_entity(self, tx_entity_tgt, tx_entity_tgt_ext, entity):
        try:
            entity = self.datawald.get_tx_staging(**entity)
            assert entity is not None, "Cannot find the entity."

            if entity.get("tx_status") == "N":
                new_entity = tx_entity_tgt(entity)
                tx_entity_tgt_ext(new_entity, entity)
                new_entity["tx_note"] = f"datawald -> {new_entity['target']}"
            else:
                new_entity = entity
        except Exception:
            log = traceback.format_exc()
            new_entity = dict(
                entity,
                **{"tx_status": "F", "tx_note": log, "tgt_id": "####"},
            )
            self.logger.exception(log)
            self.datawald.update_tx_staging(
                **{
                    "source": new_entity["source"],
                    "tx_type_src_id": new_entity["tx_type_src_id"],
                    "target": new_entity["target"],
                    "tgt_id": new_entity["tgt_id"],
                    "tx_status": new_entity["tx_status"],
                    "tx_note": new_entity["tx_note"],
                    "updated_at": new_entity["updated_at"],
                }
            )

        return new_entity

    def retrieve_entities_from_source(self, **kwargs):
        source = kwargs.get("source")
        target = kwargs.get("target")
        tx_type = kwargs.get("tx_type")
        limit = kwargs.get("limit")
        funct = kwargs.pop("funct", "insert_update_entities_to_target")
        get_entities_total = kwargs.pop("get_entities_total")
        tx_entities_src = kwargs.pop("tx_entities_src")
        tx_entities_src_ext = kwargs.pop("tx_entities_src_ext")
        validate_data = kwargs.pop("validate_data")

        sync_task = {
            "tx_type": tx_type,
            "source": source,
            "target": target,
            "entities": [],
            "funct": funct,
        }
        if kwargs.get("id"):
            sync_task.update({"id": kwargs.get("id")})

        if kwargs.get("has_offset", False):
            (offset, cut_date) = self.datawald.get_last_cute_date(
                tx_type, source, target, offset=True
            )  # Need update function.
            kwargs.update({"offset": offset, "cut_date": cut_date})

            if kwargs.get("table"):
                kwargs.update(
                    {
                        "table": kwargs.get("table"),
                    },
                )
        else:
            cut_date = self.datawald.get_last_cute_date(tx_type, source, target)
            kwargs.update({"cut_date": cut_date})

        entities = self.get_entities(
            tx_entities_src, tx_entities_src_ext, validate_data, **kwargs
        )
        for entity in entities:
            try:
                self.datawald.insert_tx_staging(
                    **{
                        "source": source,
                        "tx_type_src_id": f"{tx_type}-{entity['src_id']}",
                        "target": target,
                        "data": entity["data"],
                        "tx_status": entity.get("tx_status", "N"),
                        "tx_note": entity.get("tx_note", f"{source} -> datawald"),
                        "created_at": entity["created_at"],
                        "updated_at": entity["updated_at"],
                    }
                )
                sync_task["entities"].append(
                    {
                        "source": source,
                        "tx_type_src_id": f"{tx_type}-{entity['src_id']}",
                        "target": target,
                        "created_at": entity["created_at"],
                        "updated_at": entity["updated_at"],
                    }
                )

            except Exception:
                log = traceback.format_exc()
                self.logger.exception(log)
                self.logger.error(entity)
                raise

        if len(entities) == 0:
            return

        last_entity = max(
            entities,
            key=lambda entity: entity["updated_at"],
        )
        cut_date = last_entity["updated_at"]
        sync_task.update({"cut_date": cut_date})

        if kwargs.get("has_offset", False):
            entities_total = get_entities_total(**kwargs)
            self.logger.info(
                f"TX Type:{tx_type} Total:{entities_total} Offset:{offset} Limit:{limit} Cut Date:{cut_date}"
            )

            offset = offset + len(entities)
            if offset >= entities_total:
                offset = 0

            sync_task.update({"offset": offset})
        else:
            self.logger.info(f"TX Type:{tx_type} Limit:{limit} Cut Date:{cut_date}")

        id = self.datawald.insert_sync_task(**sync_task)
        sync_task.update({"id": id})
        return Utility.json_dumps(sync_task)

    def get_entities(
        self, tx_entities_src, tx_entities_src_ext, validate_data, **kwargs
    ):
        entities = tx_entities_src(**kwargs)
        tx_entities_src_ext(entities, **kwargs)
        for entity in entities:
            try:
                validate_data(entity, **kwargs)
            except Exception:
                log = traceback.format_exc()
                entity.update(
                    {"tx_status": "F", "tx_note": log},
                )
                self.logger.exception(log)
        return entities

    def update_sync_task(self, **kwargs):
        try:
            sync_task = self.datawald.get_sync_task(**kwargs)
            queues = [
                {"entity": entity, "count": 0} for entity in sync_task["entities"]
            ]
            entities = []
            while True:
                queue = queues.pop()
                self.logger.info(queue)
                queue["count"] += 1
                if queue["count"] > 1:
                    sleep(0.25)  # sleep(2 ** queue["count"] * 0.5)
                entity = self.datawald.get_tx_staging(**queue["entity"])
                if entity["tx_status"] != "N":
                    entities.append(
                        dict(
                            queue["entity"],
                            **{
                                k: v
                                for k, v in entity.items()
                                if k in ("tx_status", "tx_note", "updated_at")
                            },
                        )
                    )
                elif queue["count"] > 8:
                    entities.append(
                        dict(
                            queue["entity"],
                            **{
                                "tx_status": "?",
                                "tx_note": f"Not able to retrieve the result for source/tx_type_src_id({queue['entity']['source']}/{queue['entity']['tx_type_src_id']}) in 8 times.",
                            },
                        )
                    )
                else:
                    queues.append(queue)

                if len(queues) == 0:
                    break

            self.datawald.update_sync_task(**dict(kwargs, **{"entities": entities}))
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

        sync_task = self.datawald.get_sync_task(**kwargs)
        log = (
            f"({sync_task['tx_type']}:{sync_task['id']}) {sync_task['source']}->{sync_task['target']} Started at {sync_task['start_date']}, "
            + (
                f"{sync_task['sync_status']} at {sync_task['end_date']}."
                if sync_task["sync_status"] == "Completed"
                else f"{sync_task['sync_status']}!!!"
            )
        )

        if sync_task["sync_status"] == "Incompleted":
            self.logger.error(log)
            raise Exception(log)

        self.logger.info(log)
        return Utility.json_dumps(sync_task)

    def retry_sync_task(self, **kwargs):
        sync_task = self.datawald.get_sync_task(**kwargs)
        sync_task.update(
            {
                "entities": list(
                    filter(
                        lambda x: x.get("tx_status") is None
                        or x.get("tx_status") not in ("S", "I"),
                        sync_task["entities"],
                    )
                ),
                "funct": kwargs.pop("funct", "insert_update_entities_to_target"),
            }
        )

        if len(sync_task["entities"]) > 0:
            self.datawald.delete_sync_task(
                **{"tx_type": sync_task["tx_type"], "id": sync_task.pop("id")}
            )
            id = self.datawald.insert_sync_task(**sync_task)
            sync_task.update({"id": id})
        return Utility.json_dumps(sync_task)

    def transform_data(self, record, metadatas, get_cust_value=None):
        tgt = {}
        for k, v in metadatas.items():
            try:
                if v["type"] == "list":
                    value = self.extract_value(
                        get_cust_value, **self._get_params(record, v["src"][0])
                    )
                    tgt[k] = self.load_data(
                        v["funct"],
                        value,
                        data_type="list",
                        get_cust_value=get_cust_value,
                    )
                elif v["type"] == "dict":
                    value = self.extract_value(
                        get_cust_value, **self._get_params(record, v["src"][0])
                    )
                    tgt[k] = self.load_data(
                        v["funct"],
                        value,
                        data_type="dict",
                        get_cust_value=get_cust_value,
                    )
                else:
                    try:
                        funct = eval(f"lambda src: {v['funct']}")
                        src = {
                            i["label"]: self.extract_value(
                                get_cust_value, **self._get_params(record, i)
                            )
                            for i in v["src"]
                        }
                        tgt[k] = funct(src)
                    except Exception:
                        log = traceback.format_exc()
                        self.logger.error(f"{src}/{v['funct']}")
                        self.logger.exception(log)
                        tgt[k] = log
                        raise
            except Exception:
                log = traceback.format_exc()
                self.logger.error(f"{k}: {v}")
                self.logger.exception(log)
                raise
        return {
            vkey: vdata
            for vkey, vdata in tgt.items()
            if vdata is not None and vdata != ""
        }

    def load_data(self, tx, value, data_type="attribute", get_cust_value=None):
        if value is None:
            return None

        if data_type == "list":
            items = []
            for i in value:
                item = {
                    k: self.load_data(
                        (v if v["type"] == "attribute" else v["funct"]),  # tx
                        (
                            i
                            if v["type"] == "attribute"
                            else self.extract_value(
                                get_cust_value, **self._get_params(i, v["src"][0])
                            )
                        ),  # value
                        data_type=v["type"],
                        get_cust_value=get_cust_value,
                    )
                    for k, v in tx.items()
                }

                items.append(
                    {
                        vkey: vdata
                        for vkey, vdata in item.items()
                        if vdata is not None and vdata != ""
                    }
                )
            return items
        elif data_type == "dict":
            item = {
                k: self.load_data(
                    (v if v["type"] == "attribute" else v["funct"]),  # tx
                    value,
                    data_type=v["type"],
                    get_cust_value=get_cust_value,
                )
                for k, v in tx.items()
            }
            return {
                vkey: vdata
                for vkey, vdata in item.items()
                if vdata is not None and vdata != ""
            }
        else:
            try:
                funct = eval(f"lambda src: {tx['funct']}")
                src = {
                    i["label"]: self.extract_value(
                        get_cust_value, **self._get_params(value, i)
                    )
                    for i in tx["src"]
                }
                return funct(src)
            except Exception:
                log = traceback.format_exc()
                self.logger.error(f"{src}/{tx['funct']}")
                self.logger.exception(log)
                return log

    def exists(self, obj, chain):
        key = chain.pop(0)
        if obj and key in obj:
            return self.exists(obj[key], chain) if chain else obj[key]
        return None

    def _get_params(self, record, params):
        params["record"] = record
        return params

    def extract_value(self, get_cust_value, **params):
        record = params.get("record")
        key = params.get("key")
        default = params.get("default")

        if key is None:
            return default

        if key == "####":
            return record

        value = None
        if get_cust_value and key.find("@") != -1:
            value = get_cust_value(record, key)
        else:
            value = self.exists(record, key.split("|"))

        if isinstance(value, str):
            value = value.encode("ascii", "ignore")
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", "ignore")

        return default if value is None else value
