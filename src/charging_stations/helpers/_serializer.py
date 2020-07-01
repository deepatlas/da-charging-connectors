from datetime import datetime


def default(obj):
    """
    Serializing datetime object and byte identifier
    :param obj:
    :return:
    """
    if isinstance(obj, datetime):
        return {"_isoformat": obj.isoformat()}
    if isinstance(obj, bytes):
        return {"_id": obj.decode("utf8")}
    return super().default(obj)


def object_hook(obj):
    """
    Deserializing datetime object and byte identifier
    :param obj:
    :return:
    """
    _isoformat = obj.get("_isoformat")
    _id = obj.get("_id")
    if _isoformat is not None:
        return datetime.fromisoformat(_isoformat)
    if _id is not None:
        return _id.encode("utf8")
    return obj
