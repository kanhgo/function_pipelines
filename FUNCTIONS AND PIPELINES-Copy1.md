## Aim: To extract Google calendar event information and Chat Space links and transforming them into useful content assets. (Non-API apporach)

## STAGE 4
# Functions and pipelines for Processing Content Sourced from Google Calendar Events


```python
# Import required modules
import pandas as pd
import re
from ics import Calendar  # from the ics package, import the calendar class to handle icalendar data in python
from datetime import datetime   # from the datetime module, import the datetime class to create objects that represent dates and times, perform operations and format them for different outputs

```


```python

''' 
load_parse_v3 is a function that will permit the extraction of data from .ics google calendar files. 
The required content will be made accessible through calendar object attributes. The final output will be a dataframe.
Intended to be used with the standalone content team email and associated calendar i.e. where no filtering is required since each event
has been identified for logging.

Created: 13/July/2024

UPDATES:
- Handling of Nonetypes that occur if there is no orgnaizer and leads to errors
- Use of the attendees attribute to get guest count.
- ID changed to Source_ID

Input arguments
calendar: calendar object created when file is read into the python workspace
year_start: start-date year, YYYY
month_start: start-date month, M (excluding leading zeros)
day_start: start-date day, D (excluding leading zeros)
year_end: end-date year, YYYY
month_end: end-date month, M (excluding leading zeros)
day_end: end-date day, D (excluding leading zeros)

version logs:
pandas = 2.2.1
ics (Python icalendar parser) = 0.7.2

'''

def load_parse_v3(calendar, year_start, month_start, day_start, year_end, month_end, day_end):
    
    start_date = datetime(year_start, month_start, day_start) #create datetime object for the start_date
    end_date = datetime(year_end, month_end, day_end) #create datetime object for the end_date 

    extracted_events=[]
    
    for event in calendar.events:   #use of the events attribute to get a list of events
        time = event.duration   
        time_seconds = time.total_seconds() # total_seconds() is an attribute of the event.duration object
        hours = time_seconds // 3600    #this returns the rounded down value or quotient
        mins = (time_seconds % 3600) // 60    #division of the remainder by 60 to find number of minutes (rounded down)

        if start_date.date() <= event.begin.datetime.date() <= end_date.date():  #use of 'begin' to extract date/time info for the event, followed by creation of a datetime object to which the date method is applied
            organizer_obj = event.organizer   #creation of an organizer object that includes the email address of the organizer
            if organizer_obj:
                guests = event.attendees
                email = organizer_obj.email   #extraction of the email address using the 'email' attribute
                extracted_events.append({
                    "Source_ID": event.uid, 
                    "Title": event.name,
                    "Date": event.begin.datetime.date(),
                    "Duration (hh:mm)": f"{int(hours)}:{int(mins)}",
                    "Organizer": email,
                    "Extra": event.extra,
                    "Count": len(guests)})                
            else:
                pass

    df = pd.DataFrame(extracted_events)
    return df

```


```python
'''
url_extraction_v2 will be used to extract urls for the various recordings. These are housed in the event.extra object / container generated 
i.e. there is no specified attribute for extraction from the event class itself or the generated event.extra object. 

Custom script is required.
First, a string containing either the video, chat or transcript is extracted. 
This is followed by extraction of the exact url for the video, chat or transcript from the previous string. 

Created: 12/July/2024

UPDATES:
Use of ZIP to combine the multiple if/else statements into one 'for loop' where simultaneuous looping over multiple targets is facilitated.

Inputs:
df = output from the load_parse function

version logs:
pandas = 2.2.1

'''

def url_extraction_v2(df):
    df['source_url'] = ''    # Initialize the new columns with empty strings
    df['video_url'] = ''
    df['sourceC_url'] = ''
    df['chat_url'] = ''
    df['sourceT_url'] = ''
    df['transcript_url'] = ''

    df['Extra'] = df['Extra'].astype(str) # event.extra creates a container object which must be converted to str for accesibility of contents
    
    source_regex = re.compile(r'video/mp4:https?://[^\s]+')  #Create a re object that represents the pattern that contains the video url
    sourceC_regex = re.compile(r'text/plain:https?://[^\s]+')
    sourceT_regex = re.compile(r'document:https?://[^\s]+')

    video_regex = re.compile(r'https?://drive[^\s]+') # Create a re object that represents the video url pattern
    chat_regex = re.compile(r'https?://[^\s]+')
    transcript_regex = re.compile(r'https?://[^\s]+')
    
    for index, row in df.iterrows():  #iterrows allows you to iterate over each row by index and row; each row is treated as a series, with the column names acting like key
        
        vid_strings = source_regex.findall(row['Extra'])   #Find all instances of the pattern in the row being considered; returns a series
        chat_strings = sourceC_regex.findall(row['Extra'])
        trans_strings = sourceT_regex.findall(row['Extra'])

        strings = [vid_strings, chat_strings, trans_strings]
        cols = ['source_url', 'sourceC_url', 'sourceT_url']
        placeholders = ['No video', 'No chat', 'No transcript']
        
        for col, string, placeholder in zip(cols, strings, placeholders):  #use zip in the for loop to allow looping over multiple targets simutaneously
            joined = ' , '.join(string or []) # defaults to an empty list if string is None
            df.at[index, col] = joined if joined else placeholder

    for index, row in df.iterrows():
        video_urls = video_regex.findall(row['source_url']) # row['source_url'] uses source_url as the key to pull that specific data.
        chat_urls = chat_regex.findall(row['sourceC_url'])
        transcript_urls = transcript_regex.findall(row['sourceT_url'])
        
        urls = [video_urls, chat_urls, transcript_urls]
        cols_2 = ['video_url', 'chat_url', 'transcript_url']

        for col, url, placeholder in zip(cols_2, urls, placeholders):
            joined_2 = ' , '.join(url or []) # defaults to an empty list if string is None
            df.at[index, col] = joined_2 if joined_2 else placeholder

    return df
```


```python
'''
clean_part1 is to be used to complete the first set of steps in the cleaning process before manual review of small events i.e. events
that may be personal or not meant for community sharing e.g. pod meetings, skill-up sessions etc.

Actions: (i) sorting by date, (ii) dropping rows with no video, (iii) dropping duplicates, (iv) dropping pod events.

Created: 13/July/2024

Inputs:
- df: table or dataframe from previous extraction steps

Version logs:
pandas = 2.2.1

'''

def clean_part1(df):
    # sort by date; defaults to ascending.
    df = df.sort_values(by='Date')
    # drop rows with no video recording
    no_video = df[df['source_url']=='No video'].index.tolist()
    df.drop(index=no_video, axis=0, inplace=True)
    # drop duplicates
    df = df.drop_duplicates()
    # drop pod or Pod meetings
    pods = df[df['Title'].str.contains(r'pod')].index.tolist()
    Pods = df[df['Title'].str.contains(r'Pod')].index.tolist()
    df.drop(index=pods, axis=0, inplace=True)
    df.drop(index=Pods, axis=0, inplace=True)

    return df
    
```


```python
'''
pause_for_manual_check is a function designed to introduce a pause in the pipeline and permit manual review of small events that may need
to be dropped.
Identified rows are manually entered and this is followed by dropping of these rows before execution of the remainder of the pipeline i.e.
clean_part2.

Created: 14/July/2024

Inputs
df: Table or dataframe from the first stage of cleaning.

Version logs:
pandas = 2.2.1
'''
def pause_for_manual_check(df):
    # Identifying small meetings that were likely personal or not meant for general community consumption
    print("Pause and review small groups")
    display(df[df['Count'] <= 8])
    # Drop rows
    rows=input("Add rows and press enter.(Separate with commas. No spaces)")
    rows=rows.split(',')
    index=[]
    for i in rows:
        i=int(i)
        index.append(i)
    df.drop(index, axis=0, inplace=True)
    return df
```


```python
'''
clean_part2_v4 is to be used to complete the second set of steps in the cleaning process after manual review of small events i.e. events
that may be personal or not meant for community sharing e.g. pod meetings, skill-up sessions etc.

Actions: (i) drop unnecessary columns, (ii) add extra columns, (iii) order the columns, (iv) reset the index, (v) addition of IDs.

Created: 25/August/2024

Inputs:
- table_name: table or dataframe produced after manual review and deletion of "small" events.
- file_path: export file path including intended name of excel file (in quotes).
    Currently defaults to: '/Users/file_path/EXTRACTED_DATA.xlsx'
- ID_start: starting ID for current batch of calender-sourced recordings

Version logs:
pandas = 2.2.1

'''

def clean_part2_v4(df, ID_start, file_path='/Users/file_path/EXTRACTED_DATA.xlsx'):
    # drop unnecessary columns
    df.drop(columns=['Extra', 'Count', 'source_url', 'sourceC_url', 'sourceT_url'], axis=1, inplace=True)
    # add extra needed columns 
    df['Topics'] = ''
    df['Type'] = ''
    df['Sub-Type'] = ''
    df['Related Skill'] = ''
    df['Level'] = ''
    df['Comments'] = ''
    df['ID'] = ''
    df['Auto_Skill'] = ''
    df['Transcript'] = ''
    df['transcript_url_pdf'] = ''
    df['Chunks'] = ''
    df['Summary'] = ''
    # order the columns
    export = df[['ID', 'Source_ID', 'Title', 'Date', 'Duration (hh:mm)', 'Organizer', 'Topics', 'Type', 'Sub-Type', 'Auto_Skill', 'Related Skill', 
                 'Level', 'Comments', 'video_url', 'chat_url', 'transcript_url', 'transcript_url_pdf', 'Transcript', 'Chunks', 'Summary']].copy()
    # reset the index
    export.reset_index(drop=True, inplace=True)

    # insert IDs
    i = 0
    for index, rows in export.iterrows():
        export.at[index, 'ID'] = ID_start + i
        i += 1
    
    # export file in excel format
    export.to_excel(file_path, index=False)
    
    return print('Exported!')
```


```python
'''
clean_part2_v4_csv is to be used to complete the second set of steps in the cleaning process after manual review of small events i.e. events
that may be personal or not meant for community sharing e.g. pod meetings, skill-up sessions etc.

Actions: (i) drop unnecessary columns, (ii) add extra columns, (iii) order the columns, (iv) reset the index.

Created: 25/August/2024

Inputs:
- table_name: table or dataframe produced after manual review and deletion of "small" events.
- file_path: export file path including intended name of csv file (in quotes).
    Currently defaults to: '/Users/file_path/EXTRACTED_DATA.csv'

Version logs:
pandas = 2.2.1

'''

def clean_part2_v4_csv(df, ID_start, file_path='/Users/file_path/EXTRACTED_DATA.csv'):
    # drop unnecessary columns
    df.drop(columns=['Extra', 'Count', 'source_url', 'sourceC_url', 'sourceT_url'], axis=1, inplace=True)
    # add extra needed columns 
    df['Topics'] = ''
    df['Type'] = ''
    df['Sub-Type'] = ''
    df['Related Skill'] = ''
    df['Level'] = ''
    df['Comments'] = ''
    df['ID'] = ''
    df['Auto_Skill'] = ''
    df['Transcript'] = ''
    df['transcript_url_pdf'] = ''
    df['Chunks'] = ''
    df['Summary'] = ''
    # order the columns
    export = df[['ID', 'Source_ID', 'Title', 'Date', 'Duration (hh:mm)', 'Organizer', 'Topics', 'Type', 'Sub-Type', 'Auto_Skill', 'Related Skill', 
                 'Level', 'Comments', 'video_url', 'chat_url', 'transcript_url', 'transcript_url_pdf', 'Transcript', 'Chunks', 'Summary']].copy()
    # reset the index
    export.reset_index(drop=True, inplace=True)

    # insert IDs
    i = 0
    for index, rows in export.iterrows():
        export.at[index, 'ID'] = ID_start + i
        i += 1    
    
    # export file in csv format
    export.to_csv(file_path, index=False)
    
    return print('Exported!')

```

### Pipeline: Calendar data extraction, transformation and loading (local)
#### (Historic)

Each calendar ics file is imported to the python ecosystem where it is transformed by:
- parsing to convert it from its raw format to a more structured format that can be further processed
- extraction of information accessible via event attributes into a dataframe
- use of custom script to extract additional information housed in the container generated by the event attribute "extra": (i) video recordings, (ii) transcripts and (iii) chats and add to the dataframe.

It is then cleaned before being exported (loading on Google drive):
- dropping of duplicates
- dropping of events without recordings
- dropping of pod meetings
- dropping of "small" events that have been manually verified as not relevant (during a pause of the pipeline)



```python
# '''
# EXPORTING EXCEL FILES
# '''

# # read the .ics file
# with open('/Users/file_path/CALENDAR_DATA.ics', 'r') as file:
#     calendar1 = Calendar(file.read())

# year_start=2024
# month_start=1
# day_start=1
# year_end=2024
# month_end=7
# day_end=12

# ID_start = 0

# # List of functions to apply in sequence
# process_steps = [
#     (load_parse_v3, {'year_start':year_start, 'month_start':month_start, 'day_start':day_start,
#                      'year_end':year_end, 'month_end':month_end, 'day_end':day_end}),   # only key word arguments are listed i.e. not the input file
#     (url_extraction_v2, {}),
#     (clean_part1, {}),
#     (pause_for_manual_check, {}),
#     (clean_part2_v4, {'ID_start':ID_start})]
```


```python
'''
EXPORTING CSV FILES
'''

# read the .ics file
with open('/Users/file_path/CALENDAR_DATA.ics', 'r') as file:
    calendar1 = Calendar(file.read())

year_start=2024
month_start=1
day_start=1
year_end=2024
month_end=7
day_end=12

ID_start = 0

# List of functions to apply in sequence
process_steps = [
    (load_parse_v3, {'year_start':year_start, 'month_start':month_start, 'day_start':day_start,
                     'year_end':year_end, 'month_end':month_end, 'day_end':day_end}),   # only key word arguments are listed i.e. not the input file
    (url_extraction_v2, {}),
    (clean_part1, {}),
    (pause_for_manual_check, {}),
    (clean_part2_v4_csv, {'ID_start':ID_start})]
```


```python
# Execute the function pipeline
df = calendar1
for step, kwargs in process_steps:
    df = step(df, **kwargs)    # **kwargs: This indicates that the function can take 0 or more keyword argumentsn i.e. input is variable
```

### Pipeline: Calendar data extraction, transformation and loading (local)
#### (Ongoing - with independent content calendar)


```python
# '''
# EXPORTING EXCEL FILES
# '''

# # read the .ics file
# with open('/Users/file_path/CALENDAR_DATA.ics', 'r') as file:
#     calendar1 = Calendar(file.read())

# year_start=2024
# month_start=1
# day_start=1
# year_end=2024
# month_end=7
# day_end=12

# file_path = '/Users/file_path/EXTRACTED_DATA.xlsx'

# ID_start = 0

# # List of functions to apply in sequence
# process_steps = [
#     (load_parse_v3, {'year_start':year_start, 'month_start':month_start, 'day_start':day_start,
#                      'year_end':year_end, 'month_end':month_end, 'day_end':day_end}),   # only key word arguments are listed i.e. not the input file
#     (url_extraction_v2, {}),
#     (clean_part1, {}),
#     (clean_part2_v3, {'file_path':file_path, 'ID_start':ID_start})]
```


```python
'''
EXPORTING .CSV FILES
Updated approach.
'''

# read the .ics file
with open('/Users/file_path/CALENDAR_DATA.ics', 'r') as file:
    calendar1 = Calendar(file.read())

year_start=2024
month_start=1
day_start=1
year_end=2024
month_end=7
day_end=12

ID_start = 0

# List of functions to apply in sequence
process_steps = [
    (load_parse_v3, {'year_start':year_start, 'month_start':month_start, 'day_start':day_start,
                     'year_end':year_end, 'month_end':month_end, 'day_end':day_end}),   # only key word arguments are listed i.e. not the input file
    (url_extraction_v2, {}),
    (clean_part1, {}),
    (clean_part2_v4_csv, {'ID_start':ID_start})]
```


```python
# Execute the function pipeline
df = calendar1
for step, kwargs in process_steps:
    df = step(df, **kwargs)  # **kwargs: This indicates that the function can take 0 or more keyword argumentsn i.e. input is variable
```

## Functions and pipeline for generating summaries from extracted transcripts


```python
# Import required libraries
from transformers import BartTokenizer, BartForConditionalGeneration
import re
import time

```


```python
'''
Creation of a function to complete the pre-processing of the transcript.
It will allow for (i) removal of the list of attendees, (ii) removal of new lines (\n) and white spaces, 
(iii) dropping of blank or empty rows and (iv) replacement of null or empty transcript cells with empty strings.

Input:
file_path_tscript = file path for imported excel file with transcripts
sheet_name = sheet name in excel sheet
'''

def preprocess_tscript (df):
    # Removal of attendees list using a re pattern
    df['Transcript'] = df['Transcript'].astype(str)    # The use of re requires a string data type
    attendees_regex = r"Attendees\s*[\s\S]*Transcript"   
   
    df['Transcript']=[re.sub(attendees_regex, " ", row['Transcript'], flags=re.MULTILINE) if row['Transcript'] 
                      else "No Transcript" for index, row in df.iterrows()]

    # Cleaning: Removal of \n (newline indicator) from the text
    df['Transcript'] = df['Transcript'].str.replace('\n', ' ')

    # Cleaning: Removal of extra white spaces
    df['Transcript'] = df['Transcript'].str.strip() # Removal of leading and trailing white spaces
    df['Transcript'] = [re.sub(r"\s+", " ", row['Transcript']) for index, row in df.iterrows()]
    # Replacement of multi-size whitespaces with a single space 

    # Cleaning: Dropping blank rows
    blank_rows = df[df['Title'].isnull()].index.tolist()
    df.drop(index=blank_rows, axis=0, inplace=True)
    
    # Cleaning: Handling null or empty cells
    df['Transcript'] = df['Transcript'].fillna(' ')
    # Necessary to mitigate the error that would be genenrated when attempting to apply the chunk or summary
    # functions to the contents of the columns.

    return df
```


**DistilBART** is a distilled version of the BART (Bidirectional and Auto-Regressive Transformers) model. It is designed to be smaller, faster, and more efficient while maintaining a significant portion of BART's performance.
- DistilBART is particularly well-suited for tasks like summarization, where it can generate concise summaries of longer texts.
- It can also be used for other text generation tasks such as translation and text completion.

Ideal for applications where computational resources are limited, such as on mobile devices or in real-time applications.


```python
# Load the tokenizer and model for DistilBART
tokenizer = BartTokenizer.from_pretrained('sshleifer/distilbart-cnn-12-6')
model = BartForConditionalGeneration.from_pretrained('sshleifer/distilbart-cnn-12-6')
```


```python
'''
Function for tokenizing and splitting the transcripts into chunks.

Created: 25/August/2024

Inputs:
text = text to be tokenized and split. Can be a column from a dataframe.

Version logs:
pandas==2.2.2
transformers===4.38.2

'''

def split_into_chunks_v2(text, max_length=1024):
    # Verify if required tokenizer is loaded; load if necessary
    try:
        tokenizer_1
    except NameError:
        tokenizer_1 = BartTokenizer.from_pretrained('sshleifer/distilbart-cnn-12-6')
        
    # Tokenize the text
    tokens = tokenizer_1.encode(text)
    
    # Split into chunks based on the max number of tokens permissible per processing cycle
    chunks = [tokens[i:i+max_length] for i in range(0, len(tokens), max_length)]
    
    # decode the chunks back to text
    return [tokenizer_1.decode(chunk, skip_special_tokens=True) for chunk in chunks]
```


```python
'''
Creation of a function to use split_into_chunks function with my dataset and incorporate into the pipeline or
workflow.
Updated to skip cells with no transcript i.e. with "Error:" in the transscript column.

Created: 26/July/2024
Updated: 08/Sept/2024

Inputs:
df with extracted transcripts

Version logs:
pandas==2.2.2
transformers===4.38.2

'''

def get_chunks_v2(df):
    if 'Chunks' not in df.columns:
        df['Chunks'] = '' # Add an empty column for the chunks
        
    df['Transcript'] = df['Transcript'].astype(str) # ensure that contents of transcript column are string

    for index, row in df.iterrows():
        if r'Error:[\s\S]+' not in row['Transcript']:
            row['Chunks'] = split_into_chunks_v2(row['Transcript'])
        else:
            pass
 
    return df
```


```python
'''
Function for summarizing the chunks.

Created: 25/August/2024

Inputs:
text = text to be summarized. Can be a column from a dataframe.

Version logs:
pandas==2.2.2
transformers===4.38.2

'''

def summarize_text_v2(text):
    # Verify if required tokenizer is loaded; load if necessary
    try:
        tokenizer_1
    except NameError:
        tokenizer_1 = BartTokenizer.from_pretrained('sshleifer/distilbart-cnn-12-6')
    
    # Verify if required model is loaded; load if necessary
    try:
        model_1
    except NameError:
        model_1 = BartForConditionalGeneration.from_pretrained('sshleifer/distilbart-cnn-12-6')
        
    # Convert the input text into tokens (numerical representations of the text that the model can understand)
    inputs = tokenizer_1(text, return_tensors='pt', truncation=True, max_length=1024)
    
    # Generate the summary
    summary_ids = model_1.generate(inputs['input_ids'], max_length=300, min_length=30, do_sample=False)
    
    # Decode Tokens back to readable string, excluding "special tokens"
    summary = tokenizer_1.decode(summary_ids[0], skip_special_tokens=True)
    return summary
    
```


```python

def batch_sum_v2(df, pause_duration=120, file_path='/Users/file_path/SUMMARIZED_DATA.xlsx'):
    '''
    Function to summarize chunks in BATCHES to manage the computational load associated with using DistilBART for
    text summarization. A pause between batches is included to help with computaional efficiency i.e. manage issue of 
    over-heating etc. Progress messages are printed with each batch, along with a confirmation of file export.

    Created: 28/July/2024

    Inputs:
    df with chunks in required column
    file_path for exporting of updated data frame as an excel file (in quotes). Currently defaults to: (df, pause_duration=120, file_path='/Users/file_path/SUMMARIZED_DATA.xlsx):
    pause_duration represents the pause in seconds as e.g. 180 (=3 mins). Currently defaults to 120.

    Version logs:
    pandas==2.2.2
    transformers===4.38.2

    '''

    df['Summary'] = df['Summary'].astype(str)  # Explicitly stating the "str" datatype to avoid dtype errors when adding the summaries to the empty column
    
    batch_size = 10 if len(df) >= 10 else len(df)  # implicitly setting the batch size based on the size of the dataframe
    total_batches = len(df) // batch_size + (1 if len(df) % batch_size > 0 else 0)
    
    for i in range(0, len(df), batch_size):
        start_index = i
        # Avoid indexerror that would occur if the stop_index exceeds the the dataframe's length
        stop_index = min(i+batch_size, len(df))       # my initial thought was: if (i+batch_size)<len(df) else len(df) in list comprehension
        
        # Track batches being processed
        current_batch = (i // batch_size) + 1  # for example, if i = 0, current batch would be 1
        print (f"Processing batch {current_batch} of {total_batches}")
        
        for index, row in df.iloc[start_index:stop_index].iterrows():
            chunks = row['Chunks']
            summaries = [summarize_text(chunk) for chunk in chunks if chunk]
            combined_summaries = " ".join(summaries)    
            df.at[index, 'Summary'] = combined_summaries
        time.sleep(pause_duration) if stop_index < (len(df)-1) else None     # introducing a pause to manage computational load and risk of overheating etc.; passing skips the pause after the last batch
    df.to_excel(file_path, index=False)
    print ("File exported")
```


```python


def batch_sum_csv_v2(df, pause_duration=120, file_path='/Users/file_path/SUMMARIZED_DATA.csv'):
    '''
    Function to summarize chunks in BATCHES to manage the computational load associated with using DistilBART for
    text summarization. A pause between batches is included to help with computaional efficiency i.e. manage issue of 
    over-heating etc. Progress messages are printed with each batch, along with a confirmation of file export.

    Created: 28/July/2024

    Inputs:
    df with chunks in required column
    file_path for exporting of updated data frame as an excel file (in quotes). Currently defaults to: file_path='/Users/file_path/SUMMARIZED_DATA.csv':
    pause_duration represents the pause in seconds as e.g. 180 (=3 mins). Currently defaults to 120.

    Version logs:
    pandas==2.2.2
    transformers===4.38.2

    '''

    df['Summary'] = df['Summary'].astype(str)  # Explicitly stating the "str" datatype to avoid dtype errors when adding the summaries to the empty column
    
    batch_size = 10 if len(df) >= 10 else len(df)  # implicitly setting the batch size based on the size of the dataframe
    total_batches = len(df) // batch_size + (1 if len(df) % batch_size > 0 else 0)
    
    for i in range(0, len(df), batch_size):
        start_index = i
        # Avoid indexerror that would occur if the stop_index exceeds the the dataframe's length
        stop_index = min(i+batch_size, len(df))       # my initial thought was: if (i+batch_size)<len(df) else len(df) in list comprehension
        
        # Track batches being processed
        current_batch = (i // batch_size) + 1  # for example, if i = 0, current batch would be 1
        print (f"Processing batch {current_batch} of {total_batches}")
        
        for index, row in df.iloc[start_index:stop_index].iterrows():
            chunks = row['Chunks']
            summaries = [summarize_text(chunk) for chunk in chunks if chunk]
            combined_summaries = " ".join(summaries)    
            df.at[index, 'Summary'] = combined_summaries
        time.sleep(pause_duration) if stop_index < (len(df)-1) else None     # introducing a pause to manage computational load and risk of overheating etc.; passing skips the pause after the last batch
    df.to_csv(file_path, index=False)
    print ("File exported")
```


### Pipeline: Generation of summaries from extracted transcripts


```python
# Load the tokenizer and model for DistilBART
tokenizer = BartTokenizer.from_pretrained('sshleifer/distilbart-cnn-12-6')
model = BartForConditionalGeneration.from_pretrained('sshleifer/distilbart-cnn-12-6')
```


```python
'''
WORKING WITH EXCEL FILES
'''

# Define keyword arguments
file_path = '/Users/file_path/SUMMARIZED_DATA.xlsx'
pause_duration = 180

# List of functions to apply in sequence with keyword arguments
process_steps_sum = [
    (preprocess_tscript, {}),
    (get_chunks, {}),
    (batch_sum, {'file_path':file_path, 'pause_duration':pause_duration})]
```


```python

# Execute the function pipeline
df = pd.read_excel('/Users/file_path/SUMMARIZED_DATA.xlsx', 
                   sheet_name='WorkSheet')
for step, kwargs in process_steps_sum:
    df = step(df, **kwargs)   # **kwargs: This indicates that the function can take 0 or more keyword argumentsn i.e. input is variable
```

    Processing batch 1 of 1
    File exported



```python
# Load the tokenizer and model for DistilBART
tokenizer = BartTokenizer.from_pretrained('sshleifer/distilbart-cnn-12-6')
model = BartForConditionalGeneration.from_pretrained('sshleifer/distilbart-cnn-12-6')
```


```python
'''
WORKING WITH CSV FILES
'''

# Define keyword arguments
file_path = '/Users/file_path/SUMMARIZED_DATA.csv'
pause_duration = 180

# List of functions to apply in sequence with keyword arguments
process_steps_sum = [
    (preprocess_tscript, {}),
    (get_chunks, {}),
    (batch_sum_csv, {'file_path':file_path, 'pause_duration':pause_duration})]
```


```python
# Execute the function pipeline
df = pd.read_csv('/Users/file_path/SUMMARIZED_DATA.csv')
for step, kwargs in process_steps_sum:
    df = step(df, **kwargs)   # **kwargs: This indicates that the function can take 0 or more keyword argumentsn i.e. input is variable
```

    Processing batch 1 of 1
    File exported

### Automated mapping to related skill
```python

def get_skills(df, category, df_skills='/Users/file_path/Skills.csv'):
    """
    Function created to predict or map a content piece or link to a related skill. This is achieved by comparing the identified
    topics for the content pieces or links with the defined skills and goals. 
    Mindset and specialisation content pieces or links will be considered separately. Assessment of project items will be done manaully.
    
    Sentence Bert is used for sentence vectorization which produces a numeric representation of the semantics or contextual meaning of each
    sentence. Cosine similarity is used to assess the similarity of compared sentences by measuring the size of the angle between the 
    associated vectors.
    
    Inputs: 
    df = Dataframe that represents the processed content or link data from the content tracker (post determination of the main topics, types and sub-types)
    df_skills = File_path for dataframe or table with the skills, levels and goals (csv file). 
            Defaults to: '/Users/file_path/Skills.csv'
    category = general type i.e. mindset ('M') or specialization ('S').
    
    """
    # Verify if required pre-trained SBERT model is loaded; load if necessary
    try:
        model_3
    except NameError:
        model_3 = SentenceTransformer('all-mpnet-base-v2')
    
    # Get topics
    if category == "M":
        df_temp = df[df["Sub-Type"] == "Mindset"]
        df_topics = df_temp[['ID', 'Topics']].copy()
    elif category == "S":
        df_temp = df[df["Sub-Type"].isin(["Data", "Customer Success", "Design", "Sales", "Marketing", "Specialization (General)"])]
        df_topics = df_temp[['ID', 'Topics']].copy()
    
    # Remove rows without topics
    nulls = df_topics[df_topics["Topics"].isna()].index.tolist()
    df_topics.drop(index=nulls, axis=0, inplace=True)
    df_topics.reset_index(drop=True, inplace=True)
    
    # Split topics into lists
    df_topics['Topics'] = df_topics['Topics'].str.split('\n')

    # Add a column for sentence embeddings for topics using SBERT
    df_topics['SB_Embeddings'] = ''

    # Generate Sentence Embeddings for the TOPICS (vectorize the sentences)
    for index, row in df_topics.iterrows():
        embeddings = []
        if row['Topics'] and row['Topics']!=["No transcript"]:  # handle empty cells (back up) and "No transcript" 
            for sentence in row['Topics']:
                embedding = model_3.encode(sentence)
                # print(embedding)
                embeddings.append(embedding)
                # print(embeddings)
        df_topics.at[index, 'SB_Embeddings'] = embeddings
    # The above is done to allow each topic to be vectorized or embedded separately. A row will have multiple topics in the Topics cell
    
    # Get skills
    df_skills = pd.read_csv(df_skills)
    df_skills = df_skills[df_skills['Category'] == category]
    df_skills.reset_index(drop=True, inplace=True)    # Need to reset the index to avoid key errors that will occur when you try index
                                                    # and map skills to the mapped_skills list

    # Ensure level is the correct data type i.e. int
    df_skills['Level'] = df_skills['Level'].astype(int)

    # Combine skill and goal into a new column
    df_skills['skill_goal'] = df_skills['Skill'] + " " + df_skills['Goal']
    
    # Add a column for sentence embeddings of skills using SBERT
    df_skills['SB_Embeddings'] = ''

    # Generate Sentence Embeddings for the SKILLS using SKILL_GOAL (vectorize the sentences)
    for index, row in df_skills.iterrows():
        df_skills.at[index, 'SB_Embeddings'] = model_3.encode(row['skill_goal'])
    
    # Stack the mulitple rows of skill sentence vectors (embeddings) into a single 2D array, where each row represents a skill)
    skills_SB_2D = np.vstack(df_skills['SB_Embeddings'].values)      # 2D array for use in cosine sim

    
    # Create the dataframe that will hold the mapped skills (created outside of the "for loop" to ensure it is not reset with each loop)
    mapped_df = pd.DataFrame(columns=['ID', 'Topics', 'Related Skill', 'Level'])
    
    # Generate cosine similarity matrices by comparing each topic (per row) with the skills
    for index, row in df_topics.iterrows():
        # Convert the list of topic embeddings in the row to a Numpy array with each row of the array representing an embedded topic.
        embeddings_topics_array = np.array(df_topics.loc[index, 'SB_Embeddings'])

        mapped_skill_ID = []
    
        # Re-shape to 2D to match the skills and enable cosine similarity assessment
        topics_len = len(df_topics.loc[index, 'SB_Embeddings'])             # Used to define the number of rows for the 2D array
        if topics_len > 0:          # Handling the ocurrence of empty topic cells
            topics_2D = embeddings_topics_array.reshape(topics_len, -1)         # Defining the shape of the 2D array
            for i in range(topics_2D.shape[0]):     # shape[0] calls the number of rows and hence the number of iterations of the for loop required
                topic_2D = topics_2D[i].reshape(1, -1)    # Evaluating each topic indivdually but maintaining the 2D array shape needed for comparison in the cosine similarity metric
                similarity_matrix = cosine_similarity(topic_2D, skills_SB_2D)    # Evaluating the similarity of each topic (individually) against all skills
                similarity_df = pd.DataFrame(similarity_matrix, columns=[x+1 for x in range(len(df_skills))])   # COnversion of the matrix to a dataframe (1 row)
                if max(similarity_df.iloc[0]) >= 0.3:    # setting a threshold or min value of 0.3 (tried from none to 0.45)
                    mapped_skill_ID.append(similarity_df.iloc[0].idxmax())     # Generate a list skill-level IDs related to the topics in the row (0.3 minimum similarity metric)
            mapped_skill_ID = list(set(mapped_skill_ID))    # remove duplicates

        mapped_skills =[]
    
        if len(mapped_skill_ID) > 0:
            for m in mapped_skill_ID:
                mapped_skills.append(df_skills.loc[m-1,'Skill'])
            
            mapped_df.at[index, 'ID'] = row['ID']
            mapped_df.at[index, 'Topics'] = row['Topics']
            mapped_df.at[index, 'Related Skill'] = list(set(mapped_skills))     # remove duplicates
            mapped_df.at[index, 'Level'] = [f'L{df_skills.loc[(a-1),'Level']} - {mapped_skills[mapped_skill_ID.index(a)]}' for a in mapped_skill_ID]
        else:      # built-in redundancy since there should be no rows without topics included
            mapped_df.at[index, 'ID'] = row['ID']
            mapped_df.at[index, 'Topics'] = row['Topics']
            mapped_df.at[index, 'Related Skill'] = "Not mapped"
            mapped_df.at[index, 'Level'] = "Not mapped"
    
    mapped_df.to_csv('/Users/file_path/MAPPED_SKILLS.csv', index=False)
```


# Functions and pipelines for Processing Links Sourced from Google Chat Spaces

```python
# Import required modules
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

```

```python

def chat_processing_v2(df):
    """
    
    Function created to process the shared links copied from the google chat space:
    (i) separation of title and link into separate columns
    (ii) removal of irrelevant rows and columns

    Inputs:
    df - csv file containing the copied content

    Requirements:
    pandas
    re
    
    """    
    # Drop irrelevant rows
    rows_drop = df[df['Shared by'].isna()].index.tolist() #rows with null "Shared by" is used as an indicator of irrelevant row
    df.drop(index=rows_drop, axis=0, inplace=True)

    # Drop irrelevant columns
    cols_drop = df.columns[df.columns.str.contains(r'Unnamed').tolist()]
    df.drop(columns=cols_drop, axis=1, inplace=True)

    # Rename column
    df.rename(columns={'Date sharedarrow_downward':'Date shared'}, inplace=True)

    # Add needed columns
    df['Title'] = ''
    df['Link'] = ''

    # Define patterns for search
    title_pattern = re.compile(r'[\S\s]+\n')
    link_pattern = re.compile(r'https?://[\S]+')

    # Extract titles and links
    for index, row in df.iterrows():
        if row['Name']:
            df.at[index, 'Title']= (title_pattern.search(row['Name'])).group()
            df.at[index, 'Link']= (link_pattern.search(row['Name'])).group()

    # Remove /n
    df['Title'] = df['Title'].str.replace("\n", "")

    # Define required table layout
    df = df[['Title', 'Date shared', 'Shared by', 'Link']].copy()    
    
    return df

```

```python

def get_description_v3(df, Sub_Type, file_path='/Users/file_path/EXTRACTED_LINKS.csv'):
    '''
    Use of web scraping to get website descriptions from the various links to aid in the determination of topics addressed.
    Cleaning of extracted descriptions by removing (i) extra white spaces (leading, trailing and internal), (ii) \n (newline indicators)

    Inputs:
    df1 - processed links in a dataframe
    Sub_Type - Based on the chat space the links are sourced from i.e. 'Mindset', 'Data', 'Customer Success', 'Marketing', 'Design', 'Sales'
    file_path - Export file path (in quotes), defining file name as EXTRACTED_LINKS.csv. Defaults to '/Users/file_path/EXTRACTED_LINKS.csv'

    Requirements:
    pandas
    BeautifulSoup
    requests
    tqdm
    re
    '''

    # Initialize new columns for web_description, topics, type and subtype (to match content tracker)
    df['Web_Description'] = ''
    df['Topics'] = ''
    df['Type'] = ''
    df['Sub-Type'] = ''

    for row in tqdm(df.itertuples(index=True), total=len(df[df['Link'].notnull()]), desc="Processing"):
        # index=True parameter ensures that the row index is tracked. This is necessary for later updating.
        # tqdm wraps the loop and shows a progress bar that updates with each iteration.
        # total is based on the number of rows with links (and therefore # of loops) and is used by tqdm to calcuate percentage of completion
        # desc places a description before the progress bar.
        
        # Prevent processing of empty rows
        if not pd.isna(row.Link):
            try:
                # Retrieve the HTML content of the page
                response = requests.get(row.Link)
            except Exception as err:        # Managing exceptions that arise as a result of problems with the link e.g. invalid url, max connection attempts
                 print("Problem with link:", err)    # use of the variable err also for display of the error(s)

            # Define the BeautifulSoup object with the website HTML content and the HTML parser of choice
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract the description if available by searching for the <meta> tag with the attribute name="description".
            # The content attribute within this tag usually contains a summary of the page content.
            description = soup.find('meta', attrs={'name': 'description'})
            description_content = description['content'] if description else 'No description available'

            # Update the DataFrame with the extracted description and the given sub-type
            df.at[row.Index, 'Web_Description'] = description_content
            df.at[row.Index, 'Sub-Type'] = Sub_Type

    # Cleaning: Removal of \n (newline indicator) from the text
    df['Web_Description'] = df['Web_Description'].str.replace('\n', ' ')

    # Cleaning: Removal of extra white spaces
    df['Web_Description'] = df['Web_Description'].str.strip() # Removal of leading and trailing white spaces
    df['Web_Description'] = [re.sub(r"\s+", " ", row['Web_Description']) for index, row in df.iterrows()]
    # Replacement of multi-size whitespaces with a single space    
    
    export = df.copy()
    export.to_csv(file_path, index=False)
    print("File exported!")

```

### Pipeline: For processing Links Sourced from Google Chat Spaces (up to getting descriptions)

```python

# Define keyword arguments
Sub_Type = "Data"

# List of functions to apply in sequence with keyword arguments
process_steps_links = [
    (chat_processing_v2, {}),
    (get_description_v3, {'Sub_Type':Sub_Type})]

```

```python
# Execute the function pipeline
df = pd.read_csv('/Users/file_path/CHAT_LINKS.csv')
for step, kwargs in process_steps_links:
    df = step(df, **kwargs)

```

### Automated mapping to related skill
```python


def get_skills_links(df, category, df_skills='/Users/file_path/Skills.csv'):
    """
    Function created to predict or map a content piece or link to a related skill. This is achieved by comparing the identified
    topics for the content pieces or links with the defined skills and goals. 
    Mindset and specialisation content pieces or links will be considered separately. Assessment of project items will be done manaully.
    
    Sentence Bert is used for sentence vectorization which produces a numeric representation of the semantics or contextual meaning of each
    sentence. Cosine similarity is used to assess the similarity of compared sentences by measuring the size of the angle between the 
    associated vectors.
    
    Inputs: 
    df = Dataframe that represents the processed content or link data from the content tracker (post determination of the main topics, types and sub-types)
    df_skills = File_path for dataframe or table with the skills, levels and goals (csv file). 
            Defaults to: '/Users/file_path/WORKSPACE/Skills.csv'
    category = general type i.e. mindset ('M') or specialization ('S').
    
    """
    # Verify if required pre-trained SBERT model is loaded; load if necessary
    try:
        model_3
    except NameError:
        model_3 = SentenceTransformer('all-mpnet-base-v2')
    
    # Get topics
    if category == "M":
        df_temp = df[df["Sub-Type"] == "Mindset"]
        df_topics = df_temp[['ID_Links', 'Topics']].copy()
    elif category == "S":
        df_temp = df[df["Sub-Type"].isin(["Data", "Customer Success", "Design", "Sales", "Marketing", "Specialization (General)"])]
        df_topics = df_temp[['ID_Links', 'Topics']].copy()
    
    # Remove rows without topics
    nulls = df_topics[df_topics["Topics"].isna()].index.tolist()
    df_topics.drop(index=nulls, axis=0, inplace=True)
    df_topics.reset_index(drop=True, inplace=True)
    
    # Split topics into lists
    df_topics['Topics'] = df_topics['Topics'].str.split('\n')

    # Add a column for sentence embeddings for topics using SBERT
    df_topics['SB_Embeddings'] = ''

    # Generate Sentence Embeddings for the TOPICS (vectorize the sentences)
    for index, row in df_topics.iterrows():
        embeddings = []
        if row['Topics'] and row['Topics']!=["No transcript"]:  # handle empty cells (back up) and "No transcript" 
            for sentence in row['Topics']:
                embedding = model_3.encode(sentence)
                # print(embedding)
                embeddings.append(embedding)
                # print(embeddings)
        df_topics.at[index, 'SB_Embeddings'] = embeddings
    # The above is done to allow each topic to be vectorized or embedded separately. A row will have multiple topics in the Topics cell
    
    # Get skills
    df_skills = pd.read_csv(df_skills)
    df_skills = df_skills[df_skills['Category'] == category]
    df_skills.reset_index(drop=True, inplace=True)    # Need to reset the index to avoid key errors that will occur when you try index
                                                    # and map skills to the mapped_skills list

    # Ensure level is the correct data type i.e. int
    df_skills['Level'] = df_skills['Level'].astype(int)

    # Combine skill and goal into a new column
    df_skills['skill_goal'] = df_skills['Skill'] + " " + df_skills['Goal']
    
    # Add a column for sentence embeddings of skills using SBERT
    df_skills['SB_Embeddings'] = ''

    # Generate Sentence Embeddings for the SKILLS using SKILL_GOAL (vectorize the sentences)
    for index, row in df_skills.iterrows():
        df_skills.at[index, 'SB_Embeddings'] = model_3.encode(row['skill_goal'])
    
    # Stack the mulitple rows of skill sentence vectors (embeddings) into a single 2D array, where each row represents a skill)
    skills_SB_2D = np.vstack(df_skills['SB_Embeddings'].values)      # 2D array for use in cosine sim

    
    # Create the dataframe that will hold the mapped skills (created outside of the "for loop" to ensure it is not reset with each loop)
    mapped_df = pd.DataFrame(columns=['ID_Links', 'Topics', 'Related Skill', 'Level'])
    
    # Generate cosine similarity matrices by comparing each topic (per row) with the skills
    for index, row in df_topics.iterrows():
        # Convert the list of topic embeddings in the row to a Numpy array with each row of the array representing an embedded topic.
        embeddings_topics_array = np.array(df_topics.loc[index, 'SB_Embeddings'])

        mapped_skill_ID = []
    
        # Re-shape to 2D to match the skills and enable cosine similarity assessment
        topics_len = len(df_topics.loc[index, 'SB_Embeddings'])             # Used to define the number of rows for the 2D array
        if topics_len > 0:          # Handling the ocurrence of empty topic cells
            topics_2D = embeddings_topics_array.reshape(topics_len, -1)         # Defining the shape of the 2D array
            for i in range(topics_2D.shape[0]):     # shape[0] calls the number of rows and hence the number of iterations of the for loop required
                topic_2D = topics_2D[i].reshape(1, -1)    # Evaluating each topic indivdually but maintaining the 2D array shape needed for comparison in the cosine similarity metric
                similarity_matrix = cosine_similarity(topic_2D, skills_SB_2D)    # Evaluating the similarity of each topic (individually) against all skills
                similarity_df = pd.DataFrame(similarity_matrix, columns=[x+1 for x in range(len(df_skills))])   # COnversion of the matrix to a dataframe (1 row)
                if max(similarity_df.iloc[0]) >= 0.3:    # setting a threshold or min value of 0.3 (tried from none to 0.45)
                    mapped_skill_ID.append(similarity_df.iloc[0].idxmax())     # Generate a list skill-level IDs related to the topics in the row (0.3 minimum similarity metric)
            mapped_skill_ID = list(set(mapped_skill_ID))    # remove duplicates

        mapped_skills =[]
    
        if len(mapped_skill_ID) > 0:
            for m in mapped_skill_ID:
                mapped_skills.append(df_skills.loc[m-1,'Skill'])
            
            mapped_df.at[index, 'ID_Links'] = row['ID_Links']
            mapped_df.at[index, 'Topics'] = row['Topics']
            mapped_df.at[index, 'Related Skill'] = list(set(mapped_skills))     # remove duplicates
            mapped_df.at[index, 'Level'] = [f'L{df_skills.loc[(a-1),'Level']} - {mapped_skills[mapped_skill_ID.index(a)]}' for a in mapped_skill_ID]
        else:      # built-in redundancy since there should be no rows without topics included
            mapped_df.at[index, 'ID_Links'] = row['ID_Links']
            mapped_df.at[index, 'Topics'] = row['Topics']
            mapped_df.at[index, 'Related Skill'] = "Not mapped"
            mapped_df.at[index, 'Level'] = "Not mapped"
    
    mapped_df.to_csv('/Users/file_path/MAPPED_SKILLS.csv', index=False)

```
