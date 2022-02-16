"""Implement query class definition."""
from factiva.core import const
from factiva.core.tools import validate_type, validate_field_options
from factiva.news.bulknews import BulkNewsQuery


def validate_group_options(by_source, by_dimensions):
    if (isinstance(by_source, bool) and isinstance(by_dimensions, list)):
        raise ValueError(
            "The value group_by_source_code and group_dimensions are not compatible each other"
        )

class SnapshotQuery(BulkNewsQuery):
    """Implement Snapshot Query class definition."""
    limit = 0
    file_format = ''
    frequency = ''
    date_field = ''
    group_by_source_code = None
    top = 0
    group_dimensions = None

    # pylint: disable=too-many-arguments
    def __init__(self,
                 where,
                 includes=None,
                 excludes=None,
                 select_fields=None,
                 limit=0,
                 file_format=const.API_AVRO_FORMAT,
                 frequency=const.API_MONTH_PERIOD,
                 date_field=const.API_PUBLICATION_DATETIME_FIELD,
                 group_by_source_code=None,
                 group_dimensions=None,
                 top=10):
        """Initialize query class instance."""
        super().__init__(where=where,
                         includes=includes,
                         excludes=excludes,
                         select_fields=select_fields)

        validate_type(limit, int, "Unexpected value for limit")
        if limit >= 0:
            self.limit = limit
        else:
            raise ValueError("Limit value is not valid or not positive")

        validate_type(file_format, str, "Unexpected value for file_format")
        file_format = file_format.lower().strip()
        validate_field_options(file_format, const.API_EXTRACTION_FILE_FORMATS)
        self.file_format = file_format

        validate_type(frequency, str, "Unexpected value for frequency")
        frequency = frequency.upper().strip()
        validate_field_options(frequency, const.API_DATETIME_PERIODS)
        self.frequency = frequency

        validate_type(date_field, str, "Unexpected value for date_field")
        date_field = date_field.lower().strip()
        validate_field_options(date_field, const.API_DATETIME_FIELDS)
        self.date_field = date_field

        validate_group_options(group_by_source_code, group_dimensions)

        if isinstance(group_by_source_code, bool):
            self.group_by_source_code = group_by_source_code

        if isinstance(group_dimensions, list):
            self.group_dimensions = group_dimensions

        validate_type(top, int, "Unexpected value for top")
        if top >= 0:
            self.top = top
        else:
            raise ValueError('Top value is not valid')

    def get_explain_query(self):
        """Obtain Base Query."""
        query_dict = self.get_base_query()
        return query_dict

    def get_analytics_query(self):
        """Obtain analytics Query."""
        query_dict = self.get_base_query()

        self.frequency = self.frequency.upper().strip()
        validate_field_options(self.frequency, const.API_DATETIME_PERIODS)
        query_dict["query"].update({"frequency": self.frequency})

        self.date_field = self.date_field.lower().strip()
        validate_field_options(self.date_field, const.API_DATETIME_FIELDS)
        query_dict["query"].update({"date_field": self.date_field})

        validate_group_options(self.group_by_source_code, self.group_dimensions)

        if(isinstance(self.group_by_source_code, bool)):      
            query_dict["query"].update(
                {"group_by_source_code": self.group_by_source_code})
        else:
            if(self.group_dimensions):
                if(len(self.group_dimensions)<=4):
                    for option in self.group_dimensions:
                        validate_field_options(option, const.API_GROUP_DIMENSIONS_FIELDS)
                else:
                    raise ValueError("The maximiun group_dimensions options is 4")
            else:
                self.group_dimensions = []
            
            query_dict["query"].update(
                {"group_dimensions": self.group_dimensions})
        
        query_dict["query"].update({"top": self.top})
        return query_dict

    def get_extraction_query(self):
        """Obtain the string querying Factiva."""
        query_dict = self.get_base_query()

        if self.limit > 0:
            query_dict["query"].update({"limit": self.limit})

        query_dict["query"].update({"format": self.file_format})

        return query_dict

    def __repr__(self):
        """Create string representation for Query Class."""
        return self.__str__()

    def __str__(self, detailed=False, prefix='  |-', root_prefix=''):
        """Create string representation for Query Class."""
        pprop = self.__dict__.copy()

        ret_val = f'{root_prefix}{str(self.__class__)}\n'
        if detailed:
            ret_val += '\n'.join(('{}{} = {}'.format(prefix, item, pprop[item]) for item in pprop))
        else:
            ret_val += f'{prefix}where: '
            ret_val += (self.where[:77] + '...') if len(self.where) > 80 else self.where
            ret_val += f'\n{prefix}...'
            del pprop['where']
            ret_val += '\n'.join(('{}{} = {}'.format(prefix, item, pprop[item]) for item in pprop))
        return ret_val
