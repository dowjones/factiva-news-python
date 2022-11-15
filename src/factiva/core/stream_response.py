"""Implement Stream Response Class."""


class StreamResponse:
    """Represent a Stream Response Class in string format.

    Parameters
    ----------
    response: dict
        represents a given stream response by its json representation

    Raises
    ------
    ValueError: When there is an empty response

    Examples
    --------
    Create a StreamResponse
        >>> result = StreamResponse(response)
        >>> print(result)

    type: stream
    id:
        dj-synhub-extraction-*********************************
    attributes:
        job_status: JOB_STATE_RUNNING

    relationships:
        subscriptions:
                data:
                        id: dj-synhub-extraction-*********************************-filtered-*****
                        type: Subscription

                data:
                        id: dj-synhub-extraction-*********************************-filtered-*****
                        type: Subscription

                data:
                        id: dj-synhub-extraction-*********************************-filtered-*****
                        type: Subscription

    links:
        self: https://api.dowjones.com/alpha/streams/dj-synhub-extraction-*********************************

    """
    stype = ''
    id = ''
    attributes = ''
    relationships = ''
    links = ''

    def __init__(self, data=None, links=None):
        """Construct class instance."""
        if not data:
            raise ValueError('Empty value for reponse')
        self.parse_data(data)
        self.parse_links(links)

    def parse_data(self, data):
        """Parse data if it exists.

        Parameters
        ----------
        data: dict
            dict which must contain data

        Raises
        ------
        ValueError: When there is empty data

        """
        self.stype = data['type']
        # pylint: disable=invalid-name
        self.id = data['id']
        self.attributes = self.parse_object(data['attributes'])
        self.relationships = self.parse_object(data['relationships'])

    def parse_links(self, data):
        """Parse of existing links.

        Parameters
        ----------
        data: dict
            dict which may contains links

        """
        if data and data['self']:
            self.links = self.parse_object(data)

    def parse_object(self, data, level=2):
        """Parse object representation per level of identation.

        Parameters
        ----------
        data: dict
            representation of every key value pair
        level: int
            level of identation needed

        Returns
        -------
        String which contains all attributes and values
        with identation and spaces included

        """
        object_repr = ''
        idents = "\t" * level
        for index_k, index_v in data.items():
            if isinstance(index_v, dict):
                object_repr += f'{idents}{index_k}: \n{self.parse_object(index_v, level + 1)}\n'
            elif isinstance(index_v, list):
                for att in index_v:
                    object_repr += f'{idents}{index_k}: \n{self.parse_object(att, level + 1)}\n'
            else:
                object_repr += f'{idents}{index_k}: {index_v}\n'

        return object_repr

    def __repr__(self):
        """Return the repr in the class intance."""
        return '''
            -----------
            type: {}
            id: {}
            attributes: \n{}
            relationships: \n{}
            links: \n{}
            '''.format(
            self.stype,
            self.id,
            self.attributes,
            self.relationships,
            self.links,
        )
