# This module is a system for bulk managing Ledgezepplin data groups. It
# contains the following functionality:
#   1. Pull a list of legislators who are missing particular piece of 
# information (audio, email, etc.)
#   2. Update a group of legislators from a source file to a database by
#       a. merging the source file into the destination,
#       b. overwriting the destination with the source file when the seats
# match, or
#       c. clearing the destination and the writing the source file to it.
#   3. Move a group of legislators from a source database to a destination 
# database by
#       a. merging the source into the destination,
#       b. overwriting the destination with the source when the seats match, or
#       c. clearing the destination and the writing the source to it.
#   4. Delete a batch of legislators from a DB

import pymongo, datetime, sys, unicodecsv, re
from pymongo import MongoClient
from bson.objectid import ObjectId
from configobj import ConfigObj
from string import whitespace
from fuzzywuzzy import fuzz, process

# Global Variables
config          = ConfigObj('config')
merge_floor     = 60
level_list      = ['fed-upper', 'fed-lower', 'state-upper', 'state-lower']
filters_list    = ['Level', 'State']
targets_list    = ['Audio', 'Phones', 'Emails', 'Networks']
field_list      = ['__v', '_id', 'active', 'audio_path', 'country', 'date_added', 'date_modified', 'district', 'emails', 'level', 'name', 'needs_audio', 'needs_review', 'networks', 'pending_audio_path', 'pending_filename', 'phones', 'pronunciation', 'state', 'title']

template                    = {
                            	u'active': True,
                            	u'audio_path': u'',
                            	u'country': u'us',
                            	u'emails': [],
                            	u'needs_audio': False,
                            	u'needs_review': False,
                            	u'networks': [],
                            	u'pending_audio_path': u'',
                            	u'pending_filename': u'',
                            	u'phones': [],
                            	u'pronunciation': u''}
update_fields   = {}
states          = {
                    'AK': 'Alaska',
                    'AL': 'Alabama',
                    'AR': 'Arkansas',
                    'AZ': 'Arizona',
                    'CA': 'California',
                    'CO': 'Colorado',
                    'CT': 'Connecticut',
                    'DE': 'Delaware',
                    'FL': 'Florida',
                    'GA': 'Georgia',
                    'HI': 'Hawaii',
                    'IA': 'Iowa',
                    'ID': 'Idaho',
                    'IL': 'Illinois',
                    'IN': 'Indiana',
                    'KS': 'Kansas',
                    'KY': 'Kentucky',
                    'LA': 'Louisiana',
                    'MA': 'Massachusetts',
                    'MD': 'Maryland',
                    'ME': 'Maine',
                    'MI': 'Michigan',
                    'MN': 'Minnesota',
                    'MO': 'Missouri',
                    'MS': 'Mississippi',
                    'MT': 'Montana',
                    'NC': 'North Carolina',
                    'ND': 'North Dakota',
                    'NE': 'Nebraska',
                    'NH': 'New Hampshire',
                    'NJ': 'New Jersey',
                    'NM': 'New Mexico',
                    'NV': 'Nevada',
                    'NY': 'New York',
                    'OH': 'Ohio',
                    'OK': 'Oklahoma',
                    'OR': 'Oregon',
                    'PA': 'Pennsylvania',
                    'RI': 'Rhode Island',
                    'SC': 'South Carolina',
                    'SD': 'South Dakota',
                    'TN': 'Tennessee',
                    'TX': 'Texas',
                    'UT': 'Utah',
                    'VA': 'Virginia',
                    'VT': 'Vermont',
                    'WA': 'Washington',
                    'WI': 'Wisconsin',
                    'WV': 'West Virginia',
                    'WY': 'Wyoming'
                }

#### list_menu(my_list, prompt) ##############################################
# This function creates a menu from a list. It then prompts for the user to  #
# choose one of the options.                                                 #
# Return: 1 item from my_list                                                #
##############################################################################
def list_menu(my_list, prompt):
    i = 0
    menu = {}
    
    for key in my_list:
        i += 1
        menu[str(i)] = key
    
    while True: 
        options = menu.keys()
        options.sort(key=float)
        
        for entry in options: 
            print entry, menu[entry]

        selection = raw_input(prompt)
        
        if selection in options:
            break
        else:
            print "Unknown Option Selected"
            
    return menu[selection]
    

#### pull_entries(table, criteria) ###########################################
# This function queries a mongodb table for all documents matching the       #
# criteria.                                                                  #
# Return: list of dictionaries                                               #
##############################################################################
def pull_entries(table, criteria, single = False):
    result_list = []
    
    if len(criteria) < 1:
        return result_list
    elif type(criteria) is dict:
        try:
            if single:
                items   = table.find_one(criteria)
            else:
                items   = list(table.find(criteria))
        except:
            items = []
        if items is None:
            pass
        elif type(items) is dict:
            result_list.append(items)
        elif len(items) >= 1:
            result_list += items        
    else:
        for i in range(0,len(criteria)):
            try:
                if single:
                    items   = table.find_one(criteria[i])
                else:
                    items   = list(table.find(criteria[i]))
            except:
                items = []
            if items is None:
                pass
            elif type(items) is dict:
                result_list.append(items)
            elif len(items) >= 1:
                result_list += items
    
    return result_list      
    
#### pick_db()  ##############################################################
# This function uses a menu to select between databases from config          #
# Return: pymongo table                                                      #
##############################################################################
def pick_db():
    # Pick database and form connection to legislator table
    dbList      = config['db'].keys()
    database    = list_menu(config['db'], 'Choose database to work in: ')
    DBclient    = MongoClient(config['db'][database]['url'])
    activeDB    = DBclient[config['db'][database]['name']]
    legTable    = activeDB['legislators']
    return legTable
    
#### create_filters()  #######################################################
# This function uses a menus to create an lz filter                          #
# Return: list of dictionaries to use as a filter                            #
##############################################################################
def create_filters(): 
    filters             = []

    # Collect Levels
    i = 0
    level_menu = {}
    for key in level_list:
        i += 1
        level_menu[str(i)] = key
    
    options = level_menu.keys()
    options.sort()
    
    finished            = False
    print 'List of Levels'
    
    for item in options: 
        print item, level_menu[item]
        
    while not finished:
        entry           = raw_input('Enter a list of comma seperated choices (or ALL): ')
        if entry == 'ALL':
            levels      = level_list
            finished    = True
        else:
            entry           = entry.translate(None, whitespace)
            choices         = entry.split(',')
            entry           = [str(x) for x in entry]
            if set(choices) <= set(options):
                levels      = []
                for item in choices:
                    levels.append(level_menu[item])
                finished    = True
            else:
                print 'Unrecognized input'
    
    # Collect States
    short_state         = states.keys()
    finished            = False
    
    while not finished:
        entry               = raw_input('Enter a list of comma seperated list of states (or ALL): ')
        if entry == 'ALL':
            fstates         = 'ALL'
            finished        = True
        else:
            entry           = entry.translate(None, whitespace)
            choices         = entry.split(',')
            if set(choices) <= set(short_state):
                fstates     = []
                for item in choices:
                    fstates.append(item)
                finished    = True
            else:
                print 'Unrecognized input'
                
    # Combine States and Levels
    filters                     = []
    if fstates == 'ALL':
        for item in levels:
            temp_dict           = {}
            temp_dict['level']  = item
            filters.append(temp_dict)
    else:
        for state in fstates:
            for item in levels:
                temp_dict           = {}
                temp_dict['level']  = item
                temp_dict['state']  = state
                filters.append(temp_dict)
                
    return filters
    

#### create_list_auto()  #####################################################
# This function creates a list of legislators missing data using             #
# create_filters to create the filters and then sends this list to the       #
# output_list function.                                                      #
# Return: none                                                               #
##############################################################################
def create_list_auto():
    legTable        = pick_db()    
    filters         = create_filters()
    null_filter     = list_menu(targets_list, 'Choose the field to filter for empty: ')
    null_filter     = null_filter.lower()
    if null_filter == 'audio':
        for each in filters:
            each['audio_path'] = ''
    else:
        for each in filters:
            each[null_filter] = []
        
    legislators     = []
    for criteria in filters:
        legislators += pull_entries(legTable, criteria)
    
    if len(legislators) < 1:
        print 'This list is empty.'
        return
        
    description     = 'This is a list of legislators missing %s.' % null_filter
    desc            = []
    desc.append(description)
    
    audio = null_filter == 'audio'
    output_list(legislators, desc, audio)
    


#### create_list_man() #######################################################
# This function creates a list of legislators missing data using by          #
# the user to directly type a filter. It then sends this list to the         #
# output_list function.                                                      #
# Return: none                                                               #
##############################################################################
def create_list_man(filters = ''):
    legTable        = pick_db() 

    if filters == '':
        finished        = False
        while not finished:
            criteria        = raw_input('Enter custom filter: ')
            if len(filters) > 0:
                finished    = True
            else:
                print 'Bad filter.'
    
    legislators     = pull_entries(legTable, criteria)
    
    if len(legislators) < 1:
        print 'This list is empty.'
        return
        
    description     = 'This is a list of legislators matching the criteria: %s.' % criteria
    desc            = []
    desc.append(description)
    
    output_list(legislators, desc)

#### output_list(legislators, description, audio = True) #####################
# This function outputs a list of legislators which are missing information  #
# to a csv file. It prompts the user for the tile name, and puts the         #
# description arg at the top of the file, above the headers                  #
# Return: none                                                               #
##############################################################################
def output_list(legislators, description, audio = True):
    
    finished        = False
    while not finished:
        outfile     = raw_input('Enter filename for output CSV: ')
        if len(outfile) < 1:
            print 'File name too short.'
        else:
            try:
                f           = open(outfile, 'w+')
                outwriter   = unicodecsv.writer(f, encoding='utf-8')
                finished    = True
            except:
                print 'Bad file name.'
    if audio:
        headers             = ['level', 'state', 'district', 'title', 'name', 'pronunciation']
    else:
        headers             = ['level', 'state', 'district', 'name']        
    
    outwriter.writerow(description)
    outwriter.writerow(headers)
    
    for person in legislators:
        row         = []
        if 'district' not in person:
            person['district'] = ''
        for head in headers:
            row.append(person[head])
        outwriter.writerow(row)

    f.close()
    

#### move_task() #############################################################
# This function prompts the user for two databases and a filter. It then     #
# moves all documents matching the filter from one database to the other     #
# Return: none                                                               #
##############################################################################
def move_task():
    print 'Pick the database to move files from (DB A).'
    sourceTable     = pick_db()
    print 'Pick the database to move files to (DB B)'
    destTable       = pick_db()

    filters     = create_filters()
                
    move_menu       = ['Add A to B', 'Add A when no B', 'Clear B then add A']
    finished        = False
    while not finished:
        task        = list_menu(move_menu, 'Choose your move type: ')
        if task == 'Merge A into B':
            legA    = pull_entries(sourceTable, filters)
            legB    = pull_entries(destTable, filters)
            legislators = [x for x in legA if x not in legB]
            bulk_insert(destTable, legislators)
            finished = True
        elif task == 'Add A to B':
            legislators = pull_entries(sourceTable, filters)
            bulk_insert(destTable, legislators)
            finished = True
        elif task == 'Clear B then add A':
            bulk_delete(destTable, filters)
            legislators = pull_entries(sourceTable, filters)
            bulk_insert(destTable, legislators)
            finished = True

#### del_task() ##############################################################
# This function handles the deletion of documents from a database            #
# Return: none                                                               #
##############################################################################
def del_task():
    legTable        = pick_db() 
    del_menu        = ['Delete by Criteria', 'Delete from List']
    
    finished        = False
    while not finished:
        task            = list_menu(del_menu, 'How would you like to delete: ')
        if task == 'Delete by Criteria':
            filters     = create_filters()
            finished    = True
        elif task == 'Delete from List':
            filters     = del_file(legTable)
            finished    = True
    
    bulk_delete(legTable, filters)
    

########### del_file(legTable) function ######################################
# This function prompts the user for a csv list of legislators to delete. It #
# then checks these entries for district and name matching (prompting when   #
# corrections need to be made), before returning this as a list of filters.  #
# Return: list of dictionaries to be used as a filter                        #
##############################################################################
def del_file(legTable):
    
    # Pick file to gather delete information from
    finished        = False
    while not finished:
        delfile     = raw_input('Enter filename for delete list CSV: ')
        if len(delfile) < 1:
            print 'File name too short.'
        else:
            try:
                f           = open(delfile, 'r+')
                reader      = unicodecsv.reader(f, encoding='utf-8')
                headers     = reader.next()
                if set(headers) <= set(field_list):
                    finished    = True
                else:
                    print 'Unrecognized column name.'
            except:
                print 'Bad file name.'
                
    # Read file
    del_list                = []
    value_range             = {}
    for head in headers:
        value_range[head]   = []
    index                   = 0
    accept_values           = {}
    accept_values['level']  = level_list
    accept_values['state']  = states.keys()
    for row in reader:
        index               += 1
        entry               = {}
        entry['id']         = index
        for i in range(0, len(headers)):
            entry[headers[i]]  = row[i]
            if row[i] not in value_range[headers[i]]:
                value_range[headers[i]].append(row[i])
                if headers[i] in ['level','state']:
                    if row[i] not in accept_values[headers[i]]:
                        error_msg       = 'Bad %s on row %i' % (headers[i], index)
                        error_list      = ['Error', error_msg]
                        return error_list
                        
        del_list.append(entry)
    f.close()    
    
    # Check for district matched - fix when off
    update_list = []
    if set(['level', 'state', 'district']) <= set(headers):
        for level in value_range['level']:
            if level != 'fed-upper':
                list_level      = filter_dict(del_list, 'level', level)
                for state in value_range['state']:
                    list_state  = filter_dict(list_level, 'state', state)
                    if len(list_state) > 1:
                        districts   = load_districts(level, state)
                        for entry in list_state:
                            if entry['district'] not in districts:
                                if (len(entry['district']) > 3):
                                    no_match = len([i for i, x in \
                                        enumerate(districts) \
                                        if re.match(entry['district'] + r'^', x)])
                                    if no_match != 1:
                                        update_list.append(unmatched(entry, \
                                                        'district', districts))

    # Clean del_list
    for entry in update_list:
        leg                 = filter_dict(del_list, 'id', entry[0])
        if entry[2] == 'delete':
            del_list.remove(leg)
        else:
            fleg            = {}
            fleg            = leg
            fleg[entry[1]]  = entry[2]
            del_list        = [fled if x==leg else x for x in del_list]
    update_list             = []
    
    # Check for name matched - fix when off
    if set(['level', 'state', 'name']) <= set(headers):
        crit                    = {}
        for level in value_range['level']:
            list_level          = filter_dict(del_list, 'level', level)
            crit['level']       = level
            for state in value_range['state']:
                list_state      = filter_dict(list_level, 'state', state)
                crit['state']   = state
                if len(list_state) > 1:
                    leg_list    = pull_entries(legTable, crit)
                    names       = [d['name'] for d in leg_list]
                    for entry in list_state:
                        if 'district' not in entry:
                            update_list.append(unmatched(entry, 'name', names))
                            
    # Clean del_list again
    for entry in update_list:
        leg                 = filter_dict(del_list, 'id', entry[0])
        if entry[2] == 'delete':
            del_list.remove(leg)
        else:
            fleg            = {}
            fleg            = leg
            fleg[entry[1]]  = entry[2]
            del_list        = [fled if x==leg else x for x in del_list]
            
    # Drop local id from the filters
    filters = []
    for item in del_list:
        item.pop('id', None)
        if len(item['district']) < 1:
            item.pop('district', None)
        filters.append(item)
    
    return filters

#### bulk_insert(table, records) #############################################
# This function takes a pymongo table and a list of dictionaries, and adds   #
# those dictionaries to the table via bulk method.                           #
# Return: none                                                               #
##############################################################################   
def bulk_insert(table, records):
    # Do the delete
    bulk    = table.initialize_ordered_bulk_op()
    for item in records:
        bulk.insert(item)  
    result = bulk.execute()
    print
    print result
    print
    
#### bulk_delete(table, filters) #############################################
# This function takes a pymongo table and a set of filters and uses bulk     #
# process to delete any matching entries from the table.                     #
# Return: none                                                               #
##############################################################################   
def bulk_delete(table, filters):
    # Do the delete
    bulk    = table.initialize_ordered_bulk_op()
    for f in filters:
        bulk.find(f).remove()  
    result = bulk.execute()
    print
    print result
    print
    

#### load_districts(level, state = 'ALL') ####################################
# This function loads district file form the reference folder and returns it #
# as a list of strings.                                                      #
# Return: list of strings                                                    #
##############################################################################   
def load_districts(level, state):
    filename        = config['ref_path'] + 'districts.csv'
    df              = open(filename, 'r')
    r               = unicodecsv.reader(df, encoding='utf-8')

    headers         = r.next()
    lcol            = headers.index('level')   
    scol            = headers.index('state')
    dcol            = headers.index('district')
    
    districts       = []
    for row in r:
        if (row[scol] == state) and (row[lcol] == level):
            districts.append(str(row[dcol]))
    
    df.close()
    
    districts.sort()
    return districts
    

#### filter_dict(source, key, valuelist)  ####################################
# This function filters a list of dictionaries by a given key.               #
# Return: list of dictionaries                                               #
##############################################################################
def filter_dict(source, key, valuelist):
    return [dictio for dictio in source if dictio[key] in valuelist]


########### add_file(legTable) function ######################################
# This function prompts the user for a csv list of legislators to add. It    #
# then checks these entries for district matching when present (prompting    #
# when  corrections need to be made). This list of legislators is then       #
# fleshed with details from the legTable, and sent to bulk_insert to add.    #
# Return: list of dictionaries to be used as a filter                        #
##############################################################################
def add_file(legTable, merge = False):
    # Pick file to gather add information from
    finished        = False
    while not finished:
        addfile     = raw_input('Enter filename for add list CSV: ')
        if len(addfile) < 1:
            print 'File name too short.'
        else:
            try:
                f           = open(addfile, 'r+')
                reader      = unicodecsv.reader(f, encoding='utf-8')
                headers     = reader.next()
                if set(headers) <= set(field_list):
                    finished    = True
                else:
                    print 'Unrecognized column name.'
            except:
                print 'Bad file name.'
                
    # Read file
    add_list                = []
    value_range             = {}
    for head in headers:
        value_range[head]   = []
    index                   = 0
    accept_values           = {}
    accept_values['level']  = level_list
    accept_values['state']  = states.keys()
    for row in reader:
        index               += 1
        entry               = {}
        entry['id']         = index
        for i in range(0, len(headers)):
            entry[headers[i]]  = row[i]
            if row[i] not in value_range[headers[i]]:
                value_range[headers[i]].append(row[i])
                if headers[i] in ['level','state']:
                    if row[i] not in accept_values[headers[i]]:
                        error_msg       = 'Bad %s on row %i' % (headers[i], index)
                        error_list      = ['Error', error_msg]
                        return error_list
                        
        add_list.append(entry)
    f.close()    
    
    # Check for district matched - fix when off
    update_list = []
    if set(['level', 'state', 'district']) <= set(headers):
        for level in value_range['level']:
            if level != 'fed-upper':
                list_level      = filter_dict(add_list, 'level', level)
                for state in value_range['state']:
                    list_state  = filter_dict(list_level, 'state', state)
                    if len(list_state) > 1:
                        districts   = load_districts(level, state)
                        for entry in list_state:
                            if entry['district'] not in districts:
                                if (len(entry['district']) > 3):
                                    no_match = len([i for i, x in \
                                        enumerate(districts) \
                                        if re.match(entry['district'] + r'^', x)])
                                    if no_match != 1:
                                        update_list.append(unmatched(entry, \
                                                        'district', districts))


    # Clean add_list
    for entry in update_list:
        leg                 = filter_dict(add_list, 'id', entry[0])
        if entry[2] == 'delete':
            add_list.remove(leg)
        else:
            fleg            = {}
            fleg            = leg
            fleg[entry[1]]  = entry[2]
            add_list        = [fled if x==leg else x for x in add_list]
            
    for item in add_list:
        item.pop('id', None)
        if 'district' not in item:
            item['district'] = ''

    if merge:
        add_list            = merge_list(legTable, add_list)
    
    legislators             = template_fill(legTable, value_range['state'], \
                                                value_range['level'], add_list)

    bulk_insert(legTable, legislators)
    
#### merge_list(table, legs) #################################################
# This function filters a existing matches out of a list of legislators.     #
# Return: list of dictionaries as a legislator files                         #
##############################################################################
def merge_list(table, legs):
    combined_list           = []
    for doc in legs:
        crit                = {}
        cont                = True
        crit['district']    = doc['district']
        crit['level']       = doc['level']
        crit['state']       = doc['state']
        crit['name']        = doc['name']
        poss = pull_entries(table, crit)
        if len(poss) == 1:
            cont = False

        if cont:
            crit            = {}
            crit['level']   = doc['level']
            crit['state']   = doc['state']
            crit['name']    = doc['name']
            poss = pull_entries(table, crit)
            if len(poss) == 1:
                cont = False
            elif len(poss) > 1:
                choice      = unmatched(doc, 'name', poss, 0, True)
                if choice:
                    combined_list.append(doc)
                cont = False

        if cont:
            crit            = {}
            crit['level']   = doc['level']
            crit['state']   = doc['state']
            poss            = pull_entries(table, crit)
            if len(poss) > 0:
                choice      = unmatched(doc, 'name', poss, merge_floor, True)
            else:
                choice      = True
            if choice:
                combined_list.append(doc)
            cont = False
            
    return combined_list

#### template_fill(table, level_list, state_list, legs) ######################
# This function creates a fills out a list of legislators to include data    #
# from a template created from values of like documents in the legTable.     #
# Return: list of dictionaries as a legislator files                         #
############################################################################## 
def template_fill(table, state_sublist, level_sublist, legs):
    # Pull template data    
    criteria                = []
    crit                    = {}
    for level in level_sublist:
        crit                = {}
        crit['level']       = level
        for state in state_sublist:
            crit['state']   = state
            entry           = dict(crit)
            criteria.append(entry)
    
    templates               = pull_entries(table, criteria, True)
    
    mod_date = datetime.datetime.now()
    for temp in templates:
        for field in update_fields['del']:
            temp.pop(field, None)
        for field in update_fields['time']:
            temp[field]     = mod_date
        for field in update_fields['empty']:
            temp[field]     = ''
        for field in update_fields['empty_list']:
            temp[field]     = []
        for field in update_fields['true']:
            temp[field]     = True
        for field in update_fields['false']:
            temp[field]     = False

    # Expand list to legislator files
    legislators             = []
    for temp in templates:
        sub_add             = filter_dict(legs, 'level', temp['level'])
        sub_add             = filter_dict(sub_add, 'state', temp['state'])
        if len(sub_add) > 1:
            for item in sub_add:
                for field in temp:
                    item[field] = temp[field]
                legislators.append(item)         

    return legislators
 
#### unmatched(legislator, field, possibiles)  ###############################
# This function takes a takes a field from a unmatched legislator dictionary #
# and checks it against a list of possibilities. It then uses a menu to come #
# up with a fixed value and returns that.                                    #
# Return: list [id, field, fixed value]                                      #
##############################################################################    
def unmatched(legislator, field, possibiles, floor = 0, merge = False):
    result          = []
    if merge:
        leg_list    = possibiles
        name_list   = []
        for each in possibiles:
            name_list.append(each['name'])
        possibiles   = name_list
    else:
        result.append(legislator['id'])
    result.append(field)
    potentials      = []
    
    conf            = 100
    
    if len(possibiles) == 0:
        if merge:
            return True
        else:
            result.append('delete')
            return result
    if len(possibiles) <= 5:
        potentials  = possibiles
    else:
        while len(potentials) < 5:
            conf            += -5
            if conf == floor:
                return True
            potentials      = []
            for each in possibiles:
                if fuzz.ratio(legislator['field'], each) >= conf:
                    potentials.append(str(each))
                
    potentials.append('Skip')
    
    try:
        lname   = legislator['name']
    except:
        lname   = 'NR'
    try:
        lstate  = legislator['state']
    except:
        lstate  = 'NR'
    try:
        ldist   = legislator['district']
    except:
        ldist   = 'NR'
    
    print 'Name: %s / State: %s / District: %s' % (lname, lstate, ldist)
    finished    = False
    while not finished:  
        task    = list_menu(potentials, 'Choose the correct match: ')
        if merge and task == 'Skip':
            return True
        elif merge:
            return False
        if task == 'Skip':
            result.append('delete')
            return result
            finished = True
        elif task in possibiles:
            result.append(task)
            return result
            finished = True
        else:
            print 'Bad entry'

#### insert() ################################################################
# This function uses add_file to insert a list of legislators.               #
# Return: none                                                               #
##############################################################################
def dup_check():
    insert_menu = ['Merge', 'No Merge']
    finished    = False
    while not finished:
        task    = list_menu(insert_menu, 'Would you like to merge?')
        if task == 'Merge':
            merge       = True
            finished    = True
        elif task == 'No Merge':
            merge       = False
            finished    = True
    
    legTable    = pick_db()
    add_file(legTable, merge)
#### main() ##################################################################
# This function check for duplicate legislators and then exports a list      #
# Return: none                                                               #
##############################################################################
def seat_check():
    table           = pick_db()
    filters         = create_filters()
    state_list      = []
    level_list      = []
    output          = []
    
    for each in filters:
        if 'state' in each:
            if each['state'] not in state_list:
                state_list.append(each['state'])
        if each['level'] not in level_list:
            level_list.append(each['level'])
    if 'state' not in filters[0]:
        state_list  = states.keys()
        
    level_list.sort()
    state_list.sort()

    if 'fed-upper' in level_list:
        output.append('United States Senate')
        for state in state_list:
            crit            = {}
            crit['level']   = 'fed-upper'
            crit['state']   = state
            legs            = pull_entries(table, crit)
            
            if len(legs) != 2:
                line        = '%s (%i): ' % (states[state], len(legs))
                if len(legs) == 0:
                    line    += 'Empty'
                else:
                    line    += str(legs[0]['name'])
                if len(legs) > 1:
                    for i in range(1,len(legs)):
                        line    += ', %s' % str(legs[i]['name'])
                output.append(line)
                
    title_dict      = { 'fed-lower': '%s Federal House', 
                        'state-upper': '%s Senate', 
                        'state-lower': '%s House'}
    
    for crit in filters:
        if crit['level'] != 'fed-upper':
            if 'state' in crit:
                level               = str(crit['level'])
                level_name          = level.split('-')[0].title()
                state               = str(crit['state'])
                state_name          = states[state].title()
                title               = title_dict[level] % states[state]
                output.append('')
                output.append(title)
                output              += seat_list(table, state, level)
            else:
                level               = str(crit['level'])
                level_name          = level.split('-')[0].title()
                for state in state_list:  
                    state_name      = states[state].title()
                    title           = title_dict[level] % states[state]
                    output.append('')
                    output.append(title)
                    output          += seat_list(table, state, level)
                    
    finished        = False
    while not finished:
        outfile     = raw_input('Enter filename for output: ')
        if len(outfile) < 1:
            print 'File name too short.'
        else:
            try:
                f           = open(outfile, 'w')
                finished    = True
            except:
                print 'Bad file name.'
    
    for line in output:
        f.write("%s\n" % str(line))           
    f.close()
    
def seat_list(table, state, level):
    districts               = load_districts(level, state)
    districts.sort()
    output                  = []
    working                 = True
    for each in districts:
        while working:
            filt                = {}
            filt['district']    = each
            filt['state']       = state
            filt['level']       = level
            legs                = pull_entries(table, filt)
            if len(legs) < len(districts):
                fix_filt        = dict(filt)
                fix_filt.pop('district', None)                
            line                = '%s %s (%i): ' % (states[state], each, len(legs))
            if len(legs) == 0:
                line            += 'Empty'
            else:
                line            += str(legs[0]['name'])
            if len(legs) > 1:
                for i in range(1,len(legs)):
                    line        += ', %s' % str(legs[i]['name'])
            output.append(line)
            print output
            sys.exit()
        
    return output
def update_one(table, target, id_field, field, value):
    bullseye            = {}
    try:
        bullseye[id_field]  = target[id_field]
    except:
        bullseye[id_field]  = target[0][id_field]
        
    changes             = {}
    changes[field]      = value
    set_dict            = {}
    set_dict['$set']    = changes
    
    table.update(bullseye, set_dict)
def delete_one(table, target, id_field):
    bullseye            = {}
    try:
        bullseye[id_field]  = target[id_field]
    except:
        bullseye[id_field]  = target[0][id_field]
    
    table.remove(bullseye)
    
def dist_compare(table, criteria):
    legs                = pull_entries(table, criteria)
    dist_list           = value_list(legs, 'district')
    dist_calling        = load_districts(criteria['level'], criteria['state'])
    dist_calling.sort()

    print 'In DB but not in Calling: '
    temp                = set(dist_list) - set(dist_calling)
    if temp == None:
        print '     NONE'
    else:
        for item in temp:
            print '     %s' % str(item)
    print
    print 'In Calling but not in DB: '
    temp                = set(dist_calling) - set(dist_list)
    if temp == None:
        print '     NONE'
    else:
        for item in temp:
            print '     %s' % str(item)
        
    return legs
        
def value_list(list_of_dict, key_val, sort = True):
    a_list           = []

    if type(list_of_dict) is dict:
        a_list.append(list_of_dict[skey_val])
    elif type(list_of_dict) is list:
        for each in list_of_dict:
            if type(each) is not dict:
                print 'ERROR'
                print 'value_list works with a list of dictionaries'
                print 'list element was a %s' % type(each)
                sys.exit()
            a_list.append(each[key_val])
    
    
    if sort:    
        a_list.sort()
    return a_list
    
def dmatch(x, y):
    if type(x) is not str:
        return 'ERROR: first argument must be a string'
    if (type(y) is not str) and (type(y) is not list):
        return 'ERROR: second argument must be a string or a list of strings'
    if not(all(isinstance(i,str) for i in y)):
        return 'ERROR: one or more elements in y are not a string'
    if type(y) is not list:
        return x == y
    else:
        return x in y
        
def check_senate(table):
    criteria                = {}
    criteria['level']       = 'fed-upper'
    for state in states.keys():
        criteria['state']   = state
        legs                = pull_entries(table, criteria)
        count               = len(legs)
        names               = value_list(legs, 'name')
        unique              = len(names) == len(set(names))
        good_ct             = count == 2
        if not(unique) or not(good_ct):
            s               = '%s (%i/2): ' % (states[state], count)
            s               += (', ').join(names)
            print s
            
def check_house(table, fix = False):
    criteria                = {}
    criteria['level']       = 'fed-lower'
    for state in states.keys():
        criteria['state']   = state
        legs                = pull_entries(table, criteria)
        dist_calling        = load_districts('fed-lower', state)
        dist_lz             = value_list(legs, 'district')
        both_dist           = list(set(dist_calling)&set(dist_lz))
        ext_calling         = list(set(dist_calling)-set(dist_lz))
        ext_lz              = list(set(dist_lz)-set(dist_calling))
        all_dist            = list(set(dist_calling)|set(dist_lz))
        ext_leg             = []
        for dist in both_dist:
            if dist_lz.count(dist) > 1:
                ext_leg.append(dist)
        dist_desc           = {}
        header              = '\n%s Federal House of Representatives' % states[state]
        line                = '\nDistrict %s %s: %s'
        headered            = False
        all_dist.sort(key=float)
        for dist in all_dist:
            if dist in ext_calling:
                if not(headered):
                    print header
                    headered    = True
                print line % (states[state], dist, 'Not filled in LZ')
                if fix:
                    finished        = False
                    while not finished:
                        selection           = raw_input('Enter legislator name: ')
                        if selection == '':
                            print 'Skipped'
                            finished    = True
                        else:
                            new_leg             = dict(template)
                            new_leg['name']     = selection
                            new_leg['level']    = 'fed-lower'
                            new_leg['state']    = state
                            new_leg['title']    = 'Representative'
                            new_leg['district'] = dist
                            new_id              = table.insert(new_leg)
                            print 'Added %s' % selection
                            finished    = True
            elif dist in ext_lz:
                if not(headered):
                    print header
                    headered    = True
                print line % (states[state], dist, 'No match in calling')
            if dist in ext_leg:
                if not(headered):
                    print header
                    headered    = True
                name_dict       = filter_dict(legs, 'district', dist)
                names           = value_list(name_dict, 'name')
                s               = 'Multiple Legislators'
                if fix:
                    print line % (states[state], dist, s)
                    finished        = False
                    choices         = names
                    choices.append('None of the above.')
                    while not finished:  
                        task        = list_menu(choices, 'Choose the legislator: ')
                        if task in choices:
                            finished    = True
                    for entry in name_dict:
                        if entry['name'] != task:
                            delete_one(table, entry, '_id')
                else:
                    s           += ' - '
                    s           += (', ').join(names)
                    print line % (states[state], dist, s) 
def check_state(table, state):
    criteria            = {}
    criteria['state']   = state
    levels              = ['state-upper', 'state-lower']
    for level in levels:
        criteria['level']   = level
        if level == 'state-upper':
            header          = '\n%s Upper Legislation' % states[state]
        else:
            header          = '\n%s Lower Legislation' % states[state]
        line                = 'District %s: %s'
        legs                = pull_entries(table, criteria)
        dist_calling        = load_districts(level, state)
        dist_lz             = value_list(legs, 'district')
        both_dist           = list(set(dist_calling)&set(dist_lz))
        ext_calling         = list(set(dist_calling)-set(dist_lz))
        ext_lz              = list(set(dist_lz)-set(dist_calling))
        all_dist            = list(set(dist_calling)|set(dist_lz))
        ext_leg             = []
        for dist in both_dist:
            if dist_lz.count(dist) > 1:
                ext_leg.append(dist)
        dist_desc           = {}
        headered            = False
        all_dist            = mix_sort(all_dist)
        for dist in all_dist:
            if dist in ext_calling:
                if not(headered):
                    print header
                    headered    = True
                print line % (dist, 'Not filled in LZ')
            elif dist in ext_lz:
                if not(headered):
                    print header
                    headered    = True
                print line % (dist, 'No match in calling')
            if dist in ext_leg:
                if not(headered):
                    print header
                    headered    = True
                name_dict       = filter_dict(legs, 'district', dist)
                names           = value_list(name_dict, 'name')
                s               = 'Multiple Legislators - '
                s               += (', ').join(names)
                print line % (dist, s)
def mix_sort(a_list):
    numbers         = []
    strings         = []
    for item in a_list:
        try:
            float(item)
            numbers.append(item)
        except:
            strings.append(item)
    numbers.sort(key=float)
    strings.sort()
    result          = numbers
    result.extend(strings)
    return result
    
def fuzzy_district_match(table, state):
    criteria            = {}
    criteria['state']   = state
    levels              = ['state-upper', 'state-lower']
    for level in levels:
        criteria['level']   = level
        if level == 'state-upper':
            header          = '\n%s Upper Legislation' % states[state]
        else:
            header          = '\n%s Lower Legislation' % states[state]
        line                = 'District %s: %s'
        legs                = pull_entries(table, criteria)
        dist_calling        = load_districts(level, state)
        dist_lz             = value_list(legs, 'district')
        both_dist           = list(set(dist_calling)&set(dist_lz))
        ext_calling         = list(set(dist_calling)-set(dist_lz))
        ext_lz              = list(set(dist_lz)-set(dist_calling))
        all_dist            = list(set(dist_calling)|set(dist_lz))
        ext_calling         = mix_sort(ext_calling)
        ext_lz              = mix_sort(ext_lz)
        headered            = False
        for dist in ext_lz:
            print header
            correct             = fuzz_dist(dist, dist_calling)
            if correct == 'no match':
                print 'Cant match District %s' % dist
            else:
                print 'Replacing %s with %s in LZ' % (dist, correct)
                dist_calling.remove(correct)
                target              = criteria
                target['district']  = dist
                changes             = {}
                changes['district'] = correct
                set_dict            = {}
                set_dict['$set']    = changes
                table.update(target, set_dict)
                if not(headered):
                    print header
                    headered    = True
def fuzz_dist(lz, calling):
    potentials      = []
    temp            = []
    for each in calling:
        if each not in temp:
            temp.append(each)

    calling         = temp

    try:
        int(calling[0])
    except:
        not_number  = True
        
    if len(calling) == 0:
        return calling[0]
    else:
        potentials  = calling
        
    options         = []
    for each in calling:
        temp        = each.split(' ')
        try:
            assert len(temp) > 1
            int(temp[0])
            temp    = (' ').join(temp[1:])
            options.append([each, temp])
        except:
            options.append([each, each])
    
    potentials = process.extract(lz, options, processor = lambda x: x[1], limit=20)
    menu         = [x[0] for x,y in potentials]
    menu.append('No Match')
    
    print '\n\n\nLooking to match %s' % lz
    finished    = False
    while not finished:  
        task    = list_menu(menu, 'Choose the correct match: ')
        if task == 'No Match':
            return 'no match'
        elif task in calling:
            return task
        else:
            print 'Bad entry'
def snowball(table, target):
    legTable        = {}
    changes         = {}
    for db in config['db'].keys():
        database        = db
        DBclient        = MongoClient(config['db'][database]['url'])
        activeDB        = DBclient[config['db'][database]['name']]
        legTable[db]    = activeDB['legislators']
    no_audio            = not(has_audio(target))
    criteria            = {}
    criteria['name']    = target['name']
    if no_audio:
        criteria['title']   = target['title']
        finished        = False
        while not finished:
            for db in legTable:
                candidates  = pull_entries(legTable[db], criteria)
                if len(candidates) > 0:
                    for each in candidates:
                        if has_audio(each):
                            try:
                                changes['audio_path']   = each['audio_path']
                            except:
                                changes['filename']     = each['filename']
                            
                            finished    = True
                            break
                if finished:
                    criteria.pop('title')
                    break
            break

    criteria['level']   = target['level']
    
    combined_emails     = target['emails']
    just_emails         = []
    dup_list            = []
    for each in combined_emails:
        if each['address'] not in just_emails:
            just_emails.append(each['address'])
        else:
            dup_list.append(each)
    for each in dup_list:
        combined_emails.remove(each)
    for db in legTable:
        candidates  = pull_entries(legTable[db], criteria)
        if len(candidates) > 0:
            for leg in candidates:
                for each in leg['emails']:
                    if each['address'] not in just_emails:
                        combined_emails.append(each)
                        just_emails.append(each['address'])
    if combined_emails != target['emails']:
        changes['emails'] = combined_emails
    
    combined_phones     = target['phones']
    just_phones         = []
    dup_list            = []
    for each in combined_phones:
        if each['number'] not in just_phones:
            just_phones.append(each['number'])
        else:
            dup_list.append(each)
    for each in dup_list:
        combined_phones.remove(each)
    for db in legTable:
        candidates  = pull_entries(legTable[db], criteria)
        if len(candidates) > 0:
            for leg in candidates:
                for each in leg['phones']:
                    if each['number'] not in just_phones:
                        combined_phones.append(each)
                        just_phones.append(each['number'])
    if combined_phones != target['phones']:
        changes['phones'] = combined_phones
        
    combined_networks     = target['networks']
    just_networks         = []
    dup_list            = []
    for each in combined_networks:
        if each['url'] not in just_networks:
            just_networks.append(each['url'])
        else:
            dup_list.append(each)
    for each in dup_list:
        combined_networks.remove(each)
    for db in legTable:
        candidates  = pull_entries(legTable[db], criteria)
        if len(candidates) > 0:
            for leg in candidates:
                for each in leg['networks']:
                    if each['url'] not in just_networks:
                        combined_networks.append(each)
                        just_networks.append(each['url'])
    if combined_networks != target['networks']:
        changes['networks'] = combined_networks
        
    for each in changes:
        update_one(table, target, '_id', each, changes[each])
    
def has_audio(target):
    try:
        path            = target['audio_path'] != ''
    except:
        path            = False
    try:
        filename        = target['filename'] != ''
    except:
        filename        = False
    result              = path or filename
    return result
def audio_list(legislators):
    drop_list = []
    for each in legislators:
        if has_audio(each):
            drop_list.append(each)
    for each in drop_list:
        legislators.remove(each)
    result      = []
    for each in legislators:
        s       = '%s %s' % (each['title'], each['name'])
        result.append(s)
    result.sort()
    return result
def remove_dups(table, criteria):
    legislators     = pull_entries(table, criteria)
    names           = value_list(legislators, 'name')
    for each in names:
        one_name    = filter_dict(legislators, 'name', each)
        if len(one_name) > 1:
            snowball(table, one_name[0])
            for i in range(1, len(one_name)):
                delete_one(table, one_name[i], '_id')
def fill_info(table, target, sources):
    email
    
    
#### main() ##################################################################
# This function uses menus to handle choose the proper task                  #
# Return: none                                                               #
##############################################################################
def main():

    # Pick Task
    task_menu   = ['Create List from Menu', 'Create List from Manual', 
                    'Insert', 'Seat Audit', 'Move', 'Delete', 'Exit']
        # Create List: Create a List of legislators who have a blank field
        # Update: Update a batch of legislators
        # Move: Move a batch of legislators from one DB to another
        # Delete: Delete a batch of legislators from one DB
        # Exit: Leave the program
    
    finished        = False
    while not finished:
        task            = list_menu(task_menu, 'Choose your task: ')
        if task == 'Create List from Menu':
            create_list_auto()
        elif task == 'Create List from Manual':
            create_list_man()
        elif task == 'Insert':
            insert()
        elif task == 'Seat Audit':
            seat_check()
        elif task == 'Move':
            move_task()
        elif task == 'Delete':
            del_task()
        elif task == 'Exit':            
            finished    = True

