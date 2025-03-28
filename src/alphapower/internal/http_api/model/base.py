from datetime import datetime


class BaseModel:
    def _convert_fields(self, field_mapping):
        for field_name, target_type in field_mapping.items():
            value = getattr(self, field_name, None)
            if isinstance(value, dict):
                setattr(self, field_name, target_type(**value))
            elif isinstance(value, list):
                setattr(
                    self,
                    field_name,
                    [
                        target_type(**item) if isinstance(item, dict) else item
                        for item in value
                    ],
                )
            elif target_type == datetime and isinstance(value, str):
                setattr(self, field_name, datetime.fromisoformat(value))


def map_fields(data, field_mapping):
    if isinstance(data, dict):
        return {
            field_mapping.get(k, k): map_fields(v, field_mapping)
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [map_fields(item, field_mapping) for item in data]
    return data
