
# coding: utf-8

# # San Diego Map Audit and Cleaning
# 
# For Project 3 of the Data Analysis Nanodegree I have decided to look into my future home city of San Diego, CA. The following notebook will be broken up into these sections:
# 
#     1 - Sampling 
#     2 - General Audit
#     3 - Data Model
#     4 - Shaping
#     5 - Field Audits
#     6 - Cleaning Functions
#     7 - Final Preparations
# 
# ## Section 1: Sampling
# 
# The `san-diego_california.osm` file is approximately 303.8 MB large; while this is not overwhelmingly large, it is good practice to first work with a sample of your data to reduce processing time in the auditing and cleaning phases.
# 
# Below is a snippet I borrowed from Udacity that will help us create our sample from the original file. The code block will investigate every k-th top level element that matches 'node', 'way', or 'relation' and write it (and it's children) out to an outfile named 'sample.osm'.

# In[2]:

import xml.etree.cElementTree as ET
import pprint as pp
import os
import re
import json
from collections import defaultdict


# In[170]:

OSM_FILE = "san-diego_california.osm"  # Replace this with your osm file
SAMPLE_FILE = "sample.osm"

k = 10 # Parameter: take every k-th top level element

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


with open(SAMPLE_FILE, 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')

    # Write every kth top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % k == 0:
            output.write(ET.tostring(element, encoding='utf-8'))

    output.write('</osm>')


# In[3]:

original_size = os.stat('san-diego_california.osm')
sample_size = os.stat('sample.osm')
print "Original file size:\t{}\nSample file size:\t{}".format(original_size.st_size, sample_size.st_size)


# We can see that our sample file size is now approximately 30.8 MB in comparison to our 303.8 MB. This should be large enough to determine some cleaning rules and small enough that our functions will run quickly.

# ## Section 2: General Audit
# 
# Now that we have our sample, we should get a sense of what data is available to us. To do this, we will use a SAX parsing method (iterparse) to create a dictionary with tags and their counts to help determine which fields will be critical during our shaping phase.

# In[4]:

def get_tag_frequencies(file_name):
    tag_freq = defaultdict(int)

    for ev, el, in ET.iterparse(file_name):
        if el.tag == 'node' or el.tag == 'way':
            for tag in el.iter('tag'):
                tag_freq[tag.get('k')] += 1
    return tag_freq


# In[10]:

tag_freq = get_tag_frequencies('sample.osm')
print "{} unique tags in our data set\n".format(len(tag_freq))
pp.pprint(dict(tag_freq))


# 433 tags is a lot to handle but our analysis is not dependant on a lot of these fields. 
# 
# In our analysis later on, we will be looking mostly at address and amenity tag data so we will need to find which other tags relate to our interests from these 433. (Ex: we will be looking at the distribution of different types of fast food so we will need to clean the name and cuisine fields as well as amenity)
# 
# In order to help narrow down which fields we will need to include in our data model, we should do the following 2 things:
#     - Settle on our final data model
#     - Create a list of tags we are interested in for later use in our shaping function

# ## Section 3: Data Model

# Below is the data model we will be using as provided by Udacity in the Data Analyst Nanodegree.
#     
#     {
#         "id": "2406124091",
#         "type: "node",
#         "visible":"true",
#         "created": {
#                   "version":"2",
#                   "changeset":"17206049",
#                   "timestamp":"2013-08-03T16:43:42Z",
#                   "user":"linuxUser16",
#                   "uid":"1219059"
#                 },
#         "pos": [41.9757030, -87.6921867],
#         "address": {
#                   "housenumber": "5157",
#                   "postcode": "60625",
#                   "street": "North Lincoln Ave"
#                 },
#         "amenity": "restaurant",
#         "cuisine": "mexican",
#         "name": "La Cabana De Don Luis",
#         "phone": "1 (773)-271-5176"
#     }
# 
# Some entries will not have all of these tags, some will have far more; these are the tags we will be using in our analysis later so these are what we will prioritize cleaning. 
# 
# **We will still retain entries that contain most of this data (like type, id, and our created fields) as well as the additional tags that we are not interested in at this time for potential later use.**
# 
# One thing to mention is that our model will change if the type is 'way'. Ways are paths of some variety (trail, bike lane, street, etc) and while they may have some of the data above (such as all the 'created' fields) they are not likely to have definitive addresses or one [lat, lon] position. 
# 
# To accomodate for this we will add another field for 'way' types called 'node_refs' which will reference all the node id's which contain the lat, lon coordinates that define the way.
# 
# The field will look as follows:
# 
#     `"node_refs": [432432, 4332432, 432432, 454364],
# 
# We already know all nodes have clean data for id, type, the created fields, and position as this is generally not human entered, so lets focus on our address information (housenumber, postcode, street, and city), phone, amenity type, and cuisine.
# 
# To audit these fields, we will create each field a set containing all unique values associated with that field and fields that may potentially hold related values. (Ex: to create a set for the unique values associated with 'addr:postcode' we will probably want to look for postcode data in other fields with tags name 'zip') 

# ** Gathering our keys**
# 
# In order to come up with the rules for generating the set's below, I first sorted our tag_freg list from above alphabetically by tag name, I scrolled through and wrote down keys that might be of interest. 
# 
# After I completed this list, I looked for potential to automate this since it was a pain to examine 433 unique keys. I found that the keys were generally self explanatory, if we are looking for housenumber data, typically the key had 'housenumber' in it's name even if it were prefixed by something like 'addr:' or 'tiger:'. One caveat was that 'city' yielded more keys than necessary, so I had to add an extra check to make sure 'capacity' was not added.
# 
# Next I realized that since I would be parsing each tag in our sample data, I might as well add it's value to a set as well so that I could later glance at the unique values in our sample and derive cleaning rules for once our data is shaped.

# In[77]:

housenumber = set()
house_keys = set()

postcode = set()
postcode_keys = set()

street = set()
street_keys = set()

city = set()
city_keys = set()

phone = set()
phone_keys = set()

amenity = set()
amenity_keys = set()

cuisine = set()
cuisine_keys = set()

for ev, el in ET.iterparse('sample.osm'):
        tag_list = el.iter('tag')
        if len(list(tag_list)) > 0:
            for tag in el.iter('tag'):
                tag_key = tag.get('k')
                tag_val = tag.get('v')
                if 'housenumber' in tag_key:
                    housenumber.add(tag_val)
                    house_keys.add(tag_key)
                if 'postcode' in tag_key or 'zip' in tag_key:
                    postcode.add(tag_val)
                    postcode_keys.add(tag_key)
                if 'street' in tag_key:
                    street.add(tag_val)
                    street_keys.add(tag_key)
                if 'city' in tag_key and (tag_key != 'capacity'):
                    city.add(tag_val)
                    city_keys.add(tag_key)
                if 'phone' in tag_key:
                    phone.add(tag_val)
                    phone_keys.add(tag_key)
                if 'amenity' in tag_key:
                    amenity.add(tag_val)
                    amenity_keys.add(tag_key)
                if 'cuisine' in tag_key:
                    cuisine.add(tag_val)
                    cuisine_keys.add(tag_key)


# ## Section 4: Shaping
# 
# Now that we have the keys we are interested in, I thought it wise to shape our data first before cleaning, as I find it easier to have consistent field names (i.e. our zip code data will be in address.postcode rather than zip_1, zip_2, and addr:zip_1) when cleaning similar fields of data.

# In[78]:

def shape_data(map_file):
    master = []
    for ev, el in ET.iterparse(map_file):  
        if el.tag == 'node' or el.tag == 'way':
            node = {}
            node['id'] = el.get('id')
            node['type'] = el.tag
            if node['type'] == 'node':
                node['pos'] = [el.get('lat'), el.get('lon')]
            node['created'] = {'version': el.get('version'),                               'changeset': el.get('changeset'), 'user': el.get('user'),                               'uid': el.get('uid'), 'timestamp': el.get('timestamp')}
            node['address'] = {}
            for tag in el.iter('tag'):
                key = tag.get('k')
                if key in city_keys:
                    node['address']['city'] = tag.get('v')
                if key in house_keys:
                    node['address']['housenumber'] = tag.get('v')
                if key in postcode_keys:
                    node['address']['postcode'] = tag.get('v')
                if key in street_keys:
                    node['address']['street'] = tag.get('v')
                if key in phone_keys:
                    node['phone_number'] = tag.get('v')
                elif key[:4] != 'addr':
                    node[key] = tag.get('v')
            if node['type'] == 'way':
                node['node_refs'] = []
                for nd in el.iter('nd'):
                    node['node_refs'].append(nd.get('ref'))
            if len(node['address'].keys()) == 0:
                del node['address']
            master.append(node)
    return master


# In[175]:

sample = shape_data('sample.osm')


# Our data is now shaped, so let's start our field audits and move into cleaning.

# ## Section 5: Field Audits and Cleaning Functions
# 
# We already have all of our unique entries for the fields we need to audit stored in a set from our key gathering process so let's take a look at each one and determine our cleaning rules before we write our cleaning functions
# 
# We will look at the following fields:
# 
#     1 - City name
#     2 - Postcode
#     3 - Housenumber
#     4 - Streetname
#     5 - Phone number
#     6 - Amenity name/type
#     7 - Cuisine
#     8 - Fast Food Names
#     9 - Place of Worship
# 
# Since the goal of this cleaning process is to produce a quality data product, let's remind ourselves of the dimensions of quality data to remind us of what to look for in our audit.
# 
# ** Completeness, Consistency, Accuracy, Validity, Uniformity. **

# ### 5.1 City Name

# In[80]:

for item in city:
    print item


# Great news, nothing to clean here, these are all valid, complete, accurate cities within the San Diego city limit! Lets move on.

# ### 5.2 Postcode

# In[81]:

for pc in postcode:
    print pc


# We have some minor problems with postcodes here. While these values are all within the San Diego city limits, we have some values in the format XXXXX-XXXX and some that express a range of values for our ways that span multiple postal codes.
# 
# **Cleaning Rules**
#  - If an entry contains a '-' character, split the value at '-' and retain the first 5 digits
#  - If an entry contains a ':' (denoting a range) fill in the range of postal codes between the two numbers and store the postal code as a list of those values

# In[12]:

def clean_postcode(map_dict):
    for entry in map_dict:
        if 'address' in entry.keys():
            if 'postcode' in entry['address'].keys():
                if len(entry['address']['postcode']) > 5:     
                    if '-' in entry['address']['postcode']:
                        print  "{} becomes {}".format(entry['address']['postcode'], entry['address']['postcode'][:5]) 
                        entry['address']['postcode'] = entry['address']['postcode'][:5]
                        
                    if ':' in entry['address']['postcode']:
                        zip_range = entry['address']['postcode'].split(':')                       
                        print "{} becomes {}".format(entry['address']['postcode'], range(int(zip_range[0]), int(zip_range[1])))
                        entry['address']['postcode'] = range(int(zip_range[0]), int(zip_range[1]))


# ### 5.3 Housenumber

# In[82]:

for number in housenumber:
    if not number.isdigit():
        print number


# There were far too many house numbers to audit with my own eye and there are very vague rules on what house numbers (that are purely digits) are valid (ex: your house can be 111 or 6666666 and it can still be valid) so I decided for our audit, we would only look that those house numbers with non digit characters in them.
# 
# Below are the cleaning rules
# 
# Cleaning rules:
# - Convert all '.5' addresses to the valid 1/2 format
# - Entries with ';' characters express buildings with a range of addresses within. Fill in the values in this range and store as a list

# In[13]:

def clean_housenumber(map_dict):
    for entry in map_dict:
        if 'address' in entry.keys():
            if 'housenumber' in entry['address'].keys():
                if '.5' in entry['address']['housenumber']:
                    print "{} becomes {}".format(entry['address']['housenumber'], entry['address']['housenumber'].replace('.5', '1/2'))
                    entry['address']['housenumber'] = entry['address']['housenumber'].replace('.5', '1/2')
                    
                if ';' in entry['address']['housenumber']:
                    house_range = entry['address']['housenumber'].split(';')
                    start = int(house_range[0])
                    end = int(house_range[1])
                    if start > end:
                        rng = range(end, start)
                    else:
                        rng = range(start, end)
                    print "{} becomes {}".format(entry['address']['housenumber'], rng)
                    entry['address']['housenumber'] = rng


# ### 5.4 Street Name

# In[14]:

def get_road_types(road_names):
    road_type = set()
    for name in road_names:
        n = name.split()
        road_type.add(n[len(n) - 1])
    return road_type

def get_road_prefix(road_names):
    pre = set()
    for name in road_names:
        pre.add(name.split()[0])
    return pre

road_types = get_road_types(street)
road_prefix = get_road_prefix(street)


# In[84]:

for rt in road_types:
    if len(rt) < 4:
        print rt


# In[85]:

for pre in road_prefix:
    if len(pre) < 3:
        print pre


# In order to audit street names I looked immediately for problem areas like cardinal direction abbreviations in the first and last word in a street name string as well as abbreviations in the last word of the string (St., Rd., etc)
# 
# Rules
#  - In our prefix, we will need to clean out values smaller than 3 characters long with a '.'.
#  - In our suffix, we will need to clean out values matching 'Av, Ave, Ct, Pl, Dr, Ln, St'

# In[174]:

def clean_street(data):
    error = {
        'Ave' : "Avenue",
        'St' : "Street",
        "Ln" : "Lane",
        "Av" : "Avenue",
        'Pl' : "Place",
        "Dr" : "Drive",
        "Dr." : "Drive",
        'Rd' : "Road",
        "Ct" : "Court",
        "Rd." : "Road",   
    }
    for entry in data:
        if 'address' in entry.keys():
            if 'street' in entry['address'].keys():
                name = entry['address']['street'].split()
                if name[len(name) - 1] in list(error.keys()):
                    name[len(name) - 1] = error[name[len(name) - 1]]
                    new_value = " ".join(map(str, name))
                    print "{} becomes {}".format(entry['address']['street'], new_value)
                    entry['address']['street'] = new_value


# ### 5.5 Phone Number

# In[86]:

for num in phone:
    print num


# Cleaning rules:
#  - Remove all non digit characters (i.e. periods, plus sign, parentheses, dashes, etc)
#  - Take the length of the phone number, those fewer than 10 digits xxx xxx xxxx (without spaces) should be removed
#  - If the phone number has a leading 1, remove it
#  - insert a '-' character after the 3rd and 6th digit for the xxx-xxx-xxxx format

# In[366]:

def clean_phone(map_dict):
    for entry in map_dict:
        if 'phone_number' in entry.keys():
            entry['phone_number'] = re.sub('[^0-9]','', entry['phone_number'])
            if len(entry['phone_number']) < 10:
                print "Removing {}".format(entry['phone_number'])
                del entry['phone_number']
            else:
                if entry['phone_number'][0] == '1':
                    entry['phone_number'] = entry['phone_number'][1:]
                formatted = entry['phone_number'][0:3] + '-' + entry['phone_number'][3:6] + '-' + entry['phone_number'][6:]
                print "{} becomes {}".format(entry['phone_number'], formatted)
                entry['phone_number'] = formatted


# ### 5.6 Amenity

# In[88]:

for a in sorted(amenity):
    print a


# All our amenity data looks like it is in good shape. I might have some arguments as to what should constitute an amenity but that is a gripe with OpenStreetMaps. Lets move on.

# ### 5.7 Cuisine

# In[266]:

for c in sorted(cuisine):
    print c


# Note: Some entries in our data set are not fast food but retain the cuisine field (if the amenity is a restaurant, it is likely to have a cuisine field) we will clean these values even if we are not intending to use them in our analysis
# 
# Cleaning rules:
# - If the first letter is capital, replace it with a lowercase
# - If ';' or ',' is in the entry, split it and store as a list
# - strip any whitespace cause by list splits
# - strip _shop, _house from entries
# - change 'india' values to 'indian'
# - pluralize 'burger'and 'pretzel'
# - any variation on donut (doughnut, doughnuts) should be donuts

# In[398]:

import unicodedata

# convenience function for cleaning cuisine types that will be broken out into a list
def clean_list_values(val):
    if "_" in val:
        val = val.strip("_")
    if " " in val:
        val = val.strip(" ")
    return val

def clean_cuisine(map_dict):
    for entry in map_dict:
        if 'cuisine' in entry.keys():
            if isinstance(entry['cuisine'], list):
                print "Already cleaned!"
                break
            cuisine = entry['cuisine'].lower()
            if isinstance(entry['cuisine'], unicode):
                cuisine = unicodedata.normalize('NFKD', cuisine).encode('ascii','ignore')
                print "Unicode becomes {}".format(cuisine)
            if "_shop" in cuisine:
                val = cuisine[:-5]
                print "{} becomes {}".format(cuisine, val)
                cuisine = val
            if "_house" in cuisine:
                val = cuisine[:-6]
                print "{} becomes {}".format(cuisine, val)
                cuisine = val
            if "india" == cuisine:
                val = "indian"
                print "{} becomes {}".format(cuisine, val)
                cuisine = val
            if "nut" in cuisine:
                val = "donuts"
                print "{} becomes {}".format(cuisine, val)
                cuisine = val
            if "pretzel" == cuisine:
                val = "pretzels"
                print "{} becomes {}".format(cuisine, val)
                cuisine = val
            if "burger" in cuisine and 'burgers' not in cuisine:
                val = cuisine.replace('burger', 'burgers')
                print "{} becomes {}".format(cuisine, val)
                cuisine = val
            if ";" in cuisine:
                val = cuisine.split(';')
                print "{} becomes {}".format(cuisine, val)
                cuisine = val  
            if "," in cuisine:
                val = cuisine.split(',')
                val = map(clean_list_values, val)
                print "{} becomes {}".format(cuisine, val)
                cuisine = val 
            entry['cuisine'] = cuisine                   


# ### 5.8 Fast Food Names

# Since the `name` field can be used to describe the name of any node, way, or tag, I decided to take a different approach to finding our unique values (as opposed to adding them to a set when parsing tags earlier, I pull them from our shaped data).
# 
# Lets take a look at what our fast food names look like

# In[104]:

def get_set_of_ff_names(data):
    ff_names = set()

    for row in data:
        if 'amenity' in row.keys():
            if row['amenity'] == "fast_food" and 'name' in row.keys():
                ff_names.add(row['name'])
    return ff_names


# In[106]:

ff_names = get_set_of_ff_names(sample)
pp.pprint(ff_names)


# It looks like we are going to have a lot of similar values to be cleaned (see Jack in the Box and In-N-Out) I will want to have a means of identifying what needs to be cleaned programatically when we apply this to the full data set. To do so, I will simply filter out those who have the first 4 characters in common. This isn't perfect and I can already see it impacting places that start with 'The ' but it is a quick way to reduce the set that needs to be cleaned to a size that I can manually scrub.

# In[107]:

def create_list_of_names_to_clean(set_data):
    name_clean = []
    data = sorted(list(set_data))
    for x in range(1, len(data)):
        cur_name = data[x]
        prev_name = data[x - 1]
        if cur_name[:4] == prev_name[:4]:
            name_clean.append(cur_name)
    return name_clean


# In[110]:

to_clean = create_list_of_names_to_clean(ff_names)
pp.pprint(to_clean)


# As we can see from the results above, this did not do an excellent job at filtering as it missed our only other similar value (In n out). I will still keep this method around as I don't expect the number of franchises to grow (although I do anticipate more of the current values will have different spellings) this is an extremely manual means of cleaning but that is just a part of cleaning sometimes.
# 
# **Edit** When I applied this to the full data set, my filter method generated a much larger list. I worked my way through this list, identified a regular expression that would target these issues, compiled it into the dictionary you see below and formed the cleaning function.

# In[15]:

def clean_fast_food_entries(data):
    regex_dict = {
    "^Arby": "Arby's",
    "^Bombay" : "Bombay Coast Indian Tandoor & Curry Express",
    ".Green" : "Carl's Jr. / The Green Burrito",
    "^Carl.*(r|\.)$": "Carl's Jr.",
    "^Chipo" : "Chipotle Mexican Grill",
    "^Daphn" : "Daphne's California Greek Restaurant",
    "(Wiene)" : "Wienerschnitzel",
    "^Papa" : "Papa John's Pizza",
    "^Rubio" : "Rubio's Coastal Grill",
   "^Little" : "Little Caesars",
    "^Pick" : "Pick Up Stix",
   "^Jack" : "Jack in the Box",
    "^In" : "In-N-Out Burger",
    "^Five" : "Five Guys Burger and Fries",
    "^Evolution": "Evolution Fast Food",
    "^Jersey" : "Jersey Mike's Subs",
    "^Roberto" : "Roberto's Taco Shop",
    "^Santan" : "Fresh MXN Food",
    "^Subway" : "Subway",
    "^Wahoo" : "Wahoo's Fish Taco",
    "^Z" : "Zpizza"
    }
    
    for key in regex_dict.keys():
        for entry in data:
            if 'name' in entry.keys():
                if 'amenity' in entry.keys():
                    if entry['amenity'] ==  'fast_food':
                        rgx = re.compile(key)
                        if re.search(rgx, entry['name']):
                            print "{} becomes {}".format(entry['name'], regex_dict[key])
                            entry['name'] = regex_dict[key]


# ### 5.9 Places of Worship

# I wanted to get a measure of the religious presence is in San Diego mostly to compare the number of churches to my current home in Seattle, WA. Below are the audit and cleaning functions. 

# In[117]:

def get_places_of_worship(data):
    worship_set = set()
    for entry in data:
        if 'amenity' in entry.keys():
            if entry['amenity'] == 'place_of_worship':
                if 'religion' in entry.keys():
                    worship_set.add(entry['religion'])
    return worship_set


# In[120]:

get_places_of_worship(sample)


# Our sample only returns 3 distinct religions in San Diego and I found that hard to believe so I tested it on our master set later on. The data was in good shape so the only change I made was to lump the values 'unitarian_universal' in the unitarian bucket

# In[121]:

def clean_religion(data):
    for entry in data:
        if 'amenity' in entry.keys():
            if entry['amenity'] == 'place_of_worship':
                if 'religion' in entry.keys():
                    if 'unitarian_' in entry['religion']:
                        entry['religion'] = 'unitarian'


# ### Master cleaning function
# 
# Here is a convenience function for cleaning our data at once

# In[361]:

def clean_all(data):
    print "1/6 Cleaning postcode data"
    clean_postcode(data)

    print "\n2/7 Cleaning house number data"
    clean_housenumber(data)
    
    print "\n3/7 Cleaning street name data"
    clean_street(data)
    
    print "\n4/7 Cleaning phone data"
    clean_phone(data)
    
    print "\n5/7 Cleaning cuisine data"
    clean_cuisine(data)

    print "\n6/7 Cleaning fast food name data"
    clean_fast_food_entries(data)

    print "\n7/7 Cleaning religion data"
    clean_religion(data)
    
    print "All clean"


# # Cleaning, Shaping, and JSON-ifying our final output
# 
# Now that we have a sense of what is in our map data, lets apply all the cleaning functions we derived from our sample to our original map file.
# We will start by shaping the data to a python dictionary, calling our cleaning functions one-by-one, then writing this dictionary out to a JSON file.
# 
# Finally when our file is formed, we will upload it to our instance of MongoDB!

# In[400]:

master =  shape_data('san-diego_california.osm')


# In[401]:

clean_all(master)


# In[402]:

def write_to_json(data, filename):
    with open(filename, 'w') as fp:
        json.dump(data, fp)


# In[403]:

write_to_json(master, 'sd.json')

