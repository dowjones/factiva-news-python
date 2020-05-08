from factiva.news.bulknews import BulkNewsQuery


class SnapshotQuery(BulkNewsQuery):

    limit = 0
    file_format = ''
    frequency = ''
    date_field = ''
    group_by_source_code = False
    top = 0

    def __init__(
        self,
        where,
        includes=None,
        excludes=None,
        select_fields=None,
        limit=0,
        file_format='avro',
        frequency='MONTH',
        date_field='publication_datetime',
        group_by_source_code=False,
        top=10
    ):
        super().__init__(where=where, includes=includes, excludes=excludes, select_fields=select_fields)

        if type(limit) == int:
            if limit >= 0:
                self.limit = limit
            else:
                raise ValueError("Limit value is not valid or not positive")
        else:
            raise ValueError("Unexpected value for limit")

        if type(file_format) == str:
            file_format = file_format.lower().strip()
            if file_format not in ['avro', 'json', 'csv']:
                raise ValueError('Value of file_format is not within the allowed options: avro, json, csv')
            self.file_format = file_format
        else:
            raise ValueError("Unexpected value for file_format")

        if type(frequency) == str:
            frequency = frequency.upper().strip()
            if frequency not in ['DAY', 'MONTH', 'YEAR']:
                raise ValueError('Value of frequency is not within the allowed options: DAY, MONTH, YEAR')
            self.frequency = frequency
        else:
            raise ValueError("Unexpected value for frequency")

        if type(date_field) == str:
            date_field = date_field.lower().strip()
            if date_field not in ['publication_datetime', 'modification_datetime', 'ingestion_datetime']:
                raise ValueError('Value of date_field is not within the allowed options: publication_datetime, modification_datetime, ingestion_datetime')
            self.date_field = date_field
        else:
            raise ValueError("Unexpected value for date_field")

        if type(group_by_source_code) == bool:
            self.group_by_source_code = group_by_source_code
        else:
            raise ValueError("Unexpected value for group_by_source_code")

        if type(top) == int:
            if top >= 0:
                self.top = top
        else:
            raise ValueError("Unexpected value for top")

    def get_explain_query(self):
        query_dict = self.get_base_query()
        return query_dict

    def get_analytics_query(self):
        query_dict = self.get_base_query()
        query_dict['query'].update({'frequency': self.frequency})
        query_dict['query'].update({'date_field': self.date_field})
        query_dict['query'].update({'group_by_source_code': self.group_by_source_code})
        query_dict['query'].update({'top': self.top})
        return query_dict

    def get_extraction_query(self):
        query_dict = self.get_base_query()

        if self.limit > 0:
            query_dict['query'].update({'limit': self.limit})

        query_dict['query'].update({'format': self.file_format})

        return query_dict
