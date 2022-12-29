
def extract_text(txt_dict:dict) -> str:
    if 'text' in txt_dict.keys():
        return txt_dict['text']
    elif 'content' in txt_dict.keys():
        subcontent = txt_dict['content']
        if isinstance(subcontent, dict):
            return extract_text(subcontent)
        elif isinstance(subcontent, list):
            hl_part = ''
            for content_item in subcontent:
                hl_part += extract_text(content_item)
            return hl_part 
        return ''
    return ''

def extract_headline(headline_dict:dict) -> str:
    if ((not 'main' in headline_dict.keys()) or
        (not 'content' in headline_dict['main'])):
        raise ValueError('Unexpected dict structure')
    
    headline = ''
    txt_list = headline_dict['main']['content'][0]['content']
    for tst_item in txt_list:
        headline += extract_text(tst_item)

    return headline

