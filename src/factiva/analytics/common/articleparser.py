"""Functions to parse ArticleRetrieval JSON format"""

def extract_txt(txt_dict:dict or list) -> str:
    hl_part = ''

    if isinstance(txt_dict, dict):
        if 'content' in txt_dict.keys():
            subcontent = txt_dict['content']
            if isinstance(subcontent, dict):
                return extract_txt(subcontent)
            elif isinstance(subcontent, list):
                for content_item in subcontent:
                    hl_part += extract_txt(content_item)
                return hl_part 
            return ''
        elif 'text' in txt_dict.keys():
            return txt_dict['text']

    elif isinstance(txt_dict, list):
        for p_item in txt_dict:
            hl_part += extract_txt(p_item)
        return hl_part
    
    return hl_part


def extract_headline(headline_dict:dict) -> str:
    if ((not 'main' in headline_dict.keys()) or
        (not 'content' in headline_dict['main'])):
        raise ValueError('Unexpected dict structure')
    
    headline = ''
    txt_list = headline_dict['main']['content'][0]['content']
    for txt_item in txt_list:
        headline += extract_txt(txt_item)

    return headline


def extract_body(body_dict:dict, format='txt') -> str:
    if not 'content' in body_dict.keys():
        raise ValueError('Unexpected dict structure')

    content = '\n'
    p_list = body_dict['content']

    for p_item in p_list:
        if format == 'html':
            content += "\n<p style='content'>"
        content += extract_txt(p_item)
        if format == 'html':
            content += '</p>\n'
        elif format =='txt':
            content += '\n\n'
    return content
