#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import unicodedata

PHONENUM =re.compile(r'[4,5,6,8](\d{2})-(\d{3})-(\d{4})$')
POSTCODE =re.compile(r'^(\d{5})$')


mapping_addr = { "St": "Street",
            "St.": "Street",
            "Ave": "Avenue",
            "Rd." : "Road",
            "Blvd" : "Boulevard",
            "Dr" : "Drive",
            "Hwy": "Highway",
            "Rd" : "Road", 
            "Ct" : "Court",
            "Ln" : "Lane"
            }

mapping_city = { "SUnnyvale" : "Sunnyvale",
                 "Los Gato" : "Los Gatos",
                 u'San Jos\xe9' : "San Jose",
                 "cupertino" : "Cupertino",
                 "sunnyvale": "Sunnyvale",
                 "san Jose": "San Jose",
                 "san jose": "San Jose",
                 "Los Gatos, CA": "Los Gatos",
                 "Mt Hamilton": "Mount Hamilton"
                 }
direction_addr = {'N':'North','S':'South','E':'East','W':'West','N.':'North','S.':'South','E.':'East','W.':'West'}
expand_addr = {"Mt Hamilton Rd": "Mount Hamilton Road"}
street_num ={'1st':'First','2nd':'Second','3rd':'Third'}



############################################################################
#
#     These are the four cleaning functions
# update_addr_name(),update_city_name(),update_postcode(),update_phone()
# print name,' ----> ',newname is for debugging
#
############################################################################


def update_addr_name(name):
    """
    This function updates the names of streets using 'mapping_adr','direction_addr','expand_addr'
    and 'street_num' dictionaries.
  
    Args: 
      name(str) - name of the street
   
    Returns: 
      name(str) - returns the updated name or the name if no update is needed.

    """
    newname = ''
    #replace N with North,S with South etc.
    if name.split()[0] in direction_addr:
        for key,val in direction_addr.iteritems():
            name = name.replace(key,val)
    if name in expand_addr:
        newname = expand_addr[name]
#        print name,' ----> ',newname
        return (newname)
    #replace 1st with 'First' etc.
    name_list = name.split()
    for items in name_list:
        if items in street_num:
            for key,val in street_num.iteritems():
                name = name.replace(key,val)
        
    last_word = name.split()[-1]
    if last_word in mapping_addr:
        #get the words except the last one
        for n in range(len(name.split())-1):
            newname += name.split()[n]
            newname +=' '
        newname += mapping_addr[last_word]
#        print name,' ----> ',newname
        return newname
    else:
        return name


#update the city names using the 'mapping_city' and 'expand_city' 
#dictionaries

def update_city_name(name):
    """
    This function updates the city names using 'mapping_city' dictionary.
  
    Args: 
      name(str) - city name
   
    Returns: 
      name(str) - returns the updated city name or the city name if no update is needed.

    """
    new_name=''
    if name in mapping_city: 
        new_name = mapping_city[name]
#        print name,' ----> ',repr(new_name)
        return repr(new_name)
    else:
        return repr(name)



def update_postcode(postcode):
    """
    This function updates the postcodes in the 'xxxxx' format
  
    Args: 
      name(str) - postcode
   
    Returns: 
      name(str) - returns the updated 5-digit postcode 

    """        
    if postcode.split()[0] == 'CA':
#        print postcode,' ---->',postcode.split()[1]
         return postcode.split()[1]
    elif len(postcode) == 5:
        clean_code = re.findall(r'(\d{5})',postcode)
        if clean_code:
            return clean_code[0]
        else:
            return ("invalid")
    elif len(postcode) == 10:
        clean_code = re.findall(r'^(\d{5})-(\d{4})$',postcode)[0]
        if clean_code:
            return clean_code[0]
        else:
            return ("invalid")
    else:
        return ("invalid")




def update_phone(phonenum):
    """
    This function updates phonenum in the (xxx) xxx-xxxx format
  
    Args: 
      name(str) - phone num
   
    Returns: 
      name(str) - returns the updated phone num

    """
    if PHONENUM.match(phonenum):
        return phonenum
    else:
        new_num=''
        count = 0
        for i in range(len(phonenum)):
        #get the first number
            if (phonenum[i] in ['4','5','6','8']) and count == 0:
                new_num += "("
                new_num += phonenum[i]
                count +=1
            elif (count > 0) and (count <= 12):
                if phonenum[i].isalnum():
                    new_num += phonenum[i]
                    count +=1
                    if count == 3:
                        new_num+= ") "
                    if count == 6:
                        if new_num[6].isdigit():
                            new_num += "-"
    if len(new_num) > 9 and len(new_num) <=14:
#        print phonenum,' ----> ',new_num
        return new_num    
    else:
#        print 'Invalid phone number: ',phonenum
        return ('Invalid phone number')
