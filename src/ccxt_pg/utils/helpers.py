def dict_to_text(d):

    def convert(i_value):
        if isinstance(i_value, dict) or isinstance(i_value, list):
            return str(i_value)  # Convert sub-dict to string
        return i_value

    for key, value in d.items():
        d[key] = convert(value)
    return d

