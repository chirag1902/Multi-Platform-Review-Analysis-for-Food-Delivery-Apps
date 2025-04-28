[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/5GqajVEC)
# MSDS-597 Project

Group: Project-Group-19

## Project summary

Our project summary can be found:

- as a notebook on `nbviewer`

https://nbviewer.org/gist/YOUR-GH-USERNAME/????????????????????????/

OR

- as a website:

https://moran-teaching.github.io/project-repo/????????????

## Accessing data

Our raw data can be downloaded here:

[Insert link to raw data]

Our processed data can be downloaded here:

[Insert link to processed data]

NOTE: do not include your data in your git repo - it will likely be too large and cause issues.

## Python scripts / notebooks

The following scripts/notebooks were used produce the summary:

- `src/script.py`
- `notebooks/data_cleaning.ipynb`
- `notebooks/data_enrichment.ipynb`
- `notebooks/data_analysis.ipynb`

[Give a short description of what the notebooks contain, and their location in the git repo]

## Reproducibility

Provide a `requirements.txt` file with packages and versions of all python packages to run the analysis.

## Guide

### Summary

Your summary should include the following. 

Note: You do not need code in your summary - instead, reference where in your github repo the code is. The priority should be a concise, readable summary. You should include visualizations and conclusions regarding your data analysis.

1. # Data Collection and Sources
The data for this project was extracted from three major digital platforms: Reddit, Google Play Store, and Apple App Store.
We focused specifically on gathering user reviews related to three food delivery brands: Uber Eats, DoorDash, and GrubHub.
The collected data includes:
Review text (user's feedback)
Rating scores (for App Store and Play Store)
Number of upvotes (for Reddit)
Timestamps (date and time of the review)
Source platform (Reddit, Play Store, App Store)
App name (UberEats, DoorDash, GrubHub)

The extracted reviews were organized and stored in CSV file format, where each row represents one user review with associated metadata.
Data was collected as a one-time extraction for this project and is not automatically updated on a regular basis. However, the framework allows future re-scraping to keep the dataset updated if needed.

2. # Data Sources, Retrieval, and Structure
We retrieved the data primarily using official APIs and custom scraping tools for each platform:
Google Play Store: Data was collected through the Play Store API, using Python-based scraping tools to extract app reviews, ratings, and timestamps.
Apple App Store: Reviews were accessed using the App Store Scraper API, which allowed us to gather user feedback, rating information, and timestamps for the specified apps.
Reddit: We used the Pushshift API to collect relevant posts and comments mentioning Uber Eats, DoorDash, and GrubHub. Additional metadata like upvotes, timestamps, and subreddit names were captured during the scraping process.
All retrieval processes required setting up API keys, handling pagination, and implementing rate limits where necessary to avoid service disruptions. The extracted raw data was then exported into CSV files for further cleaning and analysis.

3. # Data Cleaning and Transformation
After extracting the raw data from various platforms, we performed a structured **ETL (Extract, Transform, Load)** process to transform the reviews into a clean, tidy tabular format suitable for analysis.
The main data cleaning and transformation steps included:
- **Text Normalization**:
  - Converted all text to lowercase.
  - Standardized brand mentions (e.g., "Uber Eats" → "ubereats", "Door Dash" → "doordash").
  - Removed special characters, emojis, URLs, and extra whitespace.
- **Handling Missing Values**:
  - Dropped reviews with missing critical fields (e.g., missing review text, missing timestamps).
  - Imputed or discarded incomplete metadata where necessary.
- **Data Structuring**:
  - Organized the reviews into structured **CSV files** with columns such as:
    - Review Text
    - Rating (for App Store and Play Store)
    - Upvotes (for Reddit)
    - Timestamp (datetime format)
    - Data Source (App Store / Play Store / Reddit)
    - App Name (UberEats, DoorDash, GrubHub)
- **Feature Engineering**:
  - Created new fields such as:
    - `cleaned_review`: preprocessed review text ready for NLP tasks
    - `sentiment`: sentiment classification (positive/neutral/negative)
    - `emotion_label`: emotion categorization (optional)
  - **Consistency Checks**:
  - Verified that timestamps were properly parsed into datetime formats.
  - Standardized column names across datasets to enable unified analysis later.

By following these steps, we ensured that the final datasets were **clean, consistent, complete**, and **ready for downstream analysis and visualization**.

4. # Sanity Checks and Validation
After cleaning and structuring the raw data, we performed several validation steps to ensure the quality and integrity of the final datasets:
- **Missing Values Check**:
  - Verified that critical fields like `review_text`, `timestamp`, and `data_source` had no missing values.
  - Dropped rows where essential columns were null or incomplete.
- **Row Count Consistency**:
  - Cross-checked the number of extracted reviews against expected counts during scraping to ensure no major data loss occurred during ETL.
  - Confirmed that the number of rows remained reasonable after cleaning (no unexpected row drops).
- **Date Parsing Verification**:
  - Ensured that all timestamps were successfully parsed into valid `datetime` objects without errors.
- **Duplicate Detection**:
  - Checked and removed duplicate review entries based on review text and timestamp combinations.
- **Column Type Enforcement**:
  - Verified that fields like `rating`, `upvotes`, and `sentiment` were of appropriate data types (integer, string, datetime).

While we did not use a formal `pytest` framework, we incorporated **assertion-based checks** inside our ETL scripts to automatically catch and flag data issues during preprocessing.

These validations helped ensure that the data was **accurate**, **consistent**, and **ready for robust analysis and modeling**.explain any tests you did to check data (e.g. using `pytest` to verify that no missing values are present in the tidied dataframes, verify that the resulting number of rows is reasonable)

5. # Data Enrichment
Beyond cleaning the raw reviews, we performed several **data enrichment** steps to add additional features and context to the dataset:
- **Sentiment Labeling**:
  - Used Natural Language Processing (NLP) models to assign a **sentiment** label (**positive**, **neutral**, or **negative**) to each review based on the text content.
  - This allowed us to categorize user opinions automatically without manual labeling.
- **Timestamp Normalization**:
  - Converted all timestamps into a **uniform datetime format** across platforms, making it easier to perform time-based analyses and monthly trend plots.

These enrichment steps enhanced the original datasets, making them **more informative**, **machine-readable**, and **ready for deeper sentiment, emotion, and trend analysis** across multiple dimensions.



6. # Summary Statistics

After cleaning and enriching the dataset, we calculated several **summary statistics** to better understand the overall trends and patterns in user reviews across platforms and apps.

Key summary statistics include:
- **Total Number of Reviews**:
  - The total count of reviews collected for each app (**UberEats**, **DoorDash**, **GrubHub**) from each data source (**App Store**, **Google Play**, **Reddit**).
- **Sentiment Distribution**:
  - Percentage breakdown of **positive**, **neutral**, and **negative** reviews by platform and by app.
  - This highlighted which platforms/apps had more positive feedback and where dissatisfaction was higher.
- **Ratings Distribution (where available)**:
  - Calculated the mean, median, and mode of star ratings from App Store and Play Store data.
  - This helped confirm if textual sentiments aligned with numerical ratings.

- **Upvotes Analysis (Reddit only)**:
  - Analyzed the average and maximum number of upvotes for Reddit posts mentioning the food delivery apps, giving an idea of which complaints or praises resonated most with users.

- **Monthly Trends**:
  - Summarized the number of reviews per month, showing how review volume changed over time (e.g., seasonal trends, spikes around holidays, service promotions).
These descriptive statistics laid the groundwork for more advanced analyses such as sentiment modeling, clustering, and topic extraction.  
They provided an initial **high-level view** of customer feedback patterns across multiple platforms.


7. present around 4-6 visualizations related to the data, explain trends and conclusions

You should have at least one interactive data widget.

You can include figures for example from an external notebook:
- https://quarto.org/docs/blog/posts/2023-03-17-jupyter-cell-embedding/ 
- https://quarto.org/docs/authoring/includes.html

8. at the end, display a graph of the git commit history

For team members of 2: 10 commits. Of 3: 15 commits. Of 4: 20 commits.

Your commits history elsewhere may be more dirty, but these 10-20 commits need to be clean and can be drawn as a graph.

Make sure your git graphs include author names, commit messages, date, git tags if any.

You can generate nice graphs of git commits with many tools. Among others, you could generate git-graphs using the following tools:

- in vscode: https://marketplace.visualstudio.com/items?itemName=mhutchie.git-graph
- https://stackoverflow.com/questions/1057564/pretty-git-branch-graphs
- https://www.gitkraken.com/solutions/commit-graph

### Data storage options

Some options for data storage:

- Box link (free with Rutgers account)
- Dropbox
- Google Drive

The following companies have free data storage (up to ~5 GB) for 12 months. Be careful to make sure you're within the free limits!!!

- AWS S3 https://aws.amazon.com/s3/
- Google Cloud https://cloud.google.com/free
- Microsoft Azure https://azure.microsoft.com/en-us/free/students

