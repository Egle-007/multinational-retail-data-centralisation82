import re

def remove_non_numerics(x):                            # Cleans phone number from nondigits, str.replace('[^0-9]') did not work 
    return re.sub('[^0-9]', '', x) 

def remove_alphabet(x):                                 # Cleans unwanted alphabeticcal characters 
    return re.sub('[^0-9x.,]', '', x)

def keep_alphabet(x):                                  # Keeps only alphabetical characters
    return re.sub('[^A-Za-z]', '', x)

def invalid_numbers(x):                                 
    if len(str(x)) == 11 and str(x[0]) == '0':         # If 11 digit number starts with '0', it removes that 0 and gives 10 digit number which should be working well with a country code.
        return x[1:]                                   
    elif len(str(x)) == 10:                            # UK, US, Germany phone numbers consist of 10 digits, sometimes written with 0 in the front making it 11 digits in total.
        return x
    else:                                              # Function 'invalid_numbers' picks those numbers that are outside expected length and returns 'Invalid number' instead.
        return 'Invalid number'
    
def phone_code(x):                                     # There is just 3 countries
    if x == 'GB':
        return '+44'
    elif x == 'US':
        return '+1'
    else:
        return '+49'

def multiply_values(val):
    if 'x' in val:
        val = val.replace('x', ' ')
        int1, int2 = val.split(' ')[0], val.split(' ')[1]
        new_val = int(int1)*int(int2)
        return new_val
    else: 
        return val
    
# remove_non_numerics, invalid_numbers, del_zero, phone_code