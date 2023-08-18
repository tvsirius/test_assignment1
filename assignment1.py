from peewee import Model, SqliteDatabase, CharField, IntegerField, ForeignKeyField, BooleanField

from pprint import pprint

# Connect to an SQLite database
db = SqliteDatabase('teaser_goods.db')


class categories(Model):
    id = IntegerField(primary_key=True)
    name = CharField(default='')

    class Meta:
        database = db


class teasers(Model):
    id = IntegerField(primary_key=True)
    category = ForeignKeyField(categories, backref='teasers')
    active = BooleanField(default=False)
    url = CharField(default='')
    picture = CharField(default='')
    title = CharField(default='')
    vendor = CharField(default='')
    text = CharField(default='')

    class Meta:
        database = db


def create_tables():
    db.create_tables([categories, teasers], safe=True)


def xml_parse(filename, version="1.0"):
    def str_format(value):
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            return value[1:-1]
        else:
            return value

    def parse_header(str_header: str):
        param_dict = {}
        if str_header.startswith("</"):
            open_tag = False
            str_header_process = str_header[2:-1]
        else:
            open_tag = True
            str_header_process = str_header[1:-1]
        str_header_process_split = str_header_process.split("=")
        if len(str_header_process_split) == 1:
            name = str_header_process
        else:
            name = str_header_process_split[0].split(" ")[0]
            for i in range(len(str_header_process_split) - 1):
                key = str_header_process_split[i].split(" ")[-1]
                value = str_format(" ".join(str_header_process_split[i + 1].split(" ")[:-1])) if i < len(
                    str_header_process_split) - 2 else \
                    str_format(str_header_process_split[i + 1])
                param_dict[key] = value
        return open_tag, name, param_dict

    def iter_parse(xml_file, current_element=None):
        # print(f'Iteration from {current_element}')
        if current_element == None:
            return_dict = {}
            char_line = xml_file.read(1)
            while not ("<" in char_line and ">" in char_line):
                nextchar = xml_file.read(1)
                if not nextchar:
                    raise
                char_line += nextchar
            # print(f'charline={char_line}')
            open_tag, name, param_dict = parse_header(char_line[char_line.find("<"):])
            # print(f'open_tag={open_tag}, name={name}, param_dict={param_dict}')
            if not open_tag:
                raise
            # print('NEXT ITERATION from start')
            return_dict[name] = {"_params": param_dict,
                                 **iter_parse(xml_file, name)}
            return return_dict
        else:
            return_dict = {'__name': ""}
            while True:
                char_line = xml_file.read(1)
                while not ("<" in char_line and ">" in char_line):
                    nextchar = xml_file.read(1)
                    if not nextchar:
                        raise
                    char_line += nextchar
                    if nextchar == '>' and char_line.find('<!') != -1:
                        char_line = char_line[:char_line.find('<!')]
                # print(f'charline={char_line}')
                text_body = char_line[0:char_line.find("<")].strip()
                open_tag, name, param_dict = parse_header(char_line[char_line.find("<"):])
                # print(f'open_tag={open_tag}, name={name}, param_dict={param_dict}')
                if not open_tag:
                    # print('return from this iteration')
                    return_dict["_body"] = text_body
                    # print('make some clearing')
                    if '__name' in return_dict:
                        return_dict.pop('__name')
                    if '_body' in return_dict and return_dict['_body'] == '':
                        return_dict.pop('_body')
                    return return_dict
                else:
                    # print('calling next iteration')
                    next_result = iter_parse(xml_file, name)

                    if param_dict == {} and '_body' in next_result and len(next_result) == 1:
                        item_value = next_result['_body']
                    elif '_list' in next_result and next_result['_list'] and len(next_result['_list']) > 1:
                        item_value = next_result['_list']
                    else:
                        item_value = {**param_dict, **next_result}

                    if name in return_dict:
                        return_dict['__name'] = name
                        return_dict['_list'] = [return_dict[name]]
                        return_dict.pop(name)
                        return_dict['_list'].append(item_value)
                    elif name == return_dict['__name']:
                        return_dict['_list'].append(item_value)
                    else:
                        return_dict['__name'] = name
                        return_dict[name] = item_value

    header = {}

    with open(filename, 'r') as xml_file:
        line = xml_file.readline()
        while not ("<?xml" in line and "?>" in line):
            nextline = xml_file.readline()
            if not nextline:
                return
            line += nextline
    header_end = line.find("?>") + 2
    headerline = line[line.find("<?xlm") + 7:].split("?>")[0]
    headerline = headerline.split(" ")
    for header_param in headerline:
        header[header_param.split("=")[0]] = str_format(header_param.split("=")[1])

    print(f'XML header={header}')

    if version != header['version']:
        print('Incorrect version')
        return
    with open(filename, 'r', encoding=header['encoding']) as xml_file:
        xml_file.read(header_end)
        try:
            parsed_dic = iter_parse(xml_file)
        except [TypeError,KeyError]:
            return None
    return parsed_dic



XML_FILE_NAME = "mgid_teaser_goods_export3.xml"


print(f'Parsing XML {XML_FILE_NAME}')

xml_dict = xml_parse(XML_FILE_NAME)

if not xml_dict:
    print("Error parsing XML")
    exit(1)

pprint(xml_dict)

main_key = list(xml_dict.keys())[0]

categories_data = xml_dict[main_key]['categories']
teasers_data = xml_dict[main_key]['teasers']

print('Storing data to database')

db.connect()
create_tables()

for category in categories_data:
    category_id = int(category['id'])
    if categories.get_or_none(id=category_id) is None:
        categories.create(id=category_id,
                          name=category['_body'])
    else:
        print(f'Error adding category {category} to categories table, id already exist')

for teaser in teasers_data:
    teaser_id = int(teaser['id'])
    if teasers.get_or_none(id=teaser_id) is None:
        category = categories.get_or_none(id=teaser['categoryId'])
        if category is not None:
            active = teaser['active'] == 'true'
            teasers.create(id=teaser_id,
                           category=category,
                           active=active,
                           picture=teaser['picture'],
                           text=teaser['text'],
                           title=teaser['title'],
                           url=teaser['url'],
                           vendor=teaser['vendor'])
        else:
            print(f'Error adding teaser {teaser}, category not found')
    else:
        print(f'Error adding teaser {teaser}, id already exist')

print('Storing data complete')

db.close()