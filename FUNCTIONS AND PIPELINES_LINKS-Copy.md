## Functions and Pipelines for Processing Links Sourced from Google Chat Spaces
Links and titles are separated, followed by extraction of the web-based description of the link.


```python
# Requirements
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

def get_description_v3(df, Sub_Type, file_path='default_export_file-path/EXTRACTED_LINKS.csv'):
    '''
    Use of web scraping to get website descriptions from the various links to aid in the determination of topics addressed.
    Cleaning of extracted descriptions by removing (i) extra white spaces (leading, trailing and internal), (ii) \n (newline indicators)

    Inputs:
    df1 - processed links in a dataframe
    Sub_Type - Based on the chat space the links are sourced from i.e. 'Mindset', 'Data', 'Customer Success', 'Marketing', 'Design', 'Sales'
    file_path - Export file path (in quotes), defining file name as EXTRACTED_LINKS.csv. Defaults to 'default_export_file-path/EXTRACTED_LINKS.csv'

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
df = pd.read_csv('import_file-path/CHAT_LINKS.csv')
for step, kwargs in process_steps_links:
    df = step(df, **kwargs)
```
