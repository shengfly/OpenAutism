import os
import time
import json
from dotenv import load_dotenv
from Bio import Entrez

# ================= CONFIGURATION =================
# Load credentials from .env file
load_dotenv()
Entrez.email = os.getenv("NCBI_EMAIL")
Entrez.api_key = os.getenv("NCBI_API_KEY")

# Directory to store the raw data
SAVE_DIR = "autism_full_database"
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)

# Comprehensive Search Query covering ASD, Autism, and all related historical terms
EXTENDED_QUERY = (
    '("Autism Spectrum Disorder"[MeSH] OR "Autism"[Title/Abstract] OR '
    '"ASD"[Title/Abstract] OR "Autistic"[Title/Abstract] OR '
    '"Asperger"[Title/Abstract] OR "Pervasive Developmental Disorder"[Title/Abstract] OR '
    '"Kanner Syndrome"[Title/Abstract] OR "Childhood Disintegrative Disorder"[Title/Abstract])'
)

def download_data_for_year(year):
    """
    Downloads all metadata for autism-related papers published in a specific year.
    Uses NCBI History Server (WebEnv) for stable large-scale data retrieval.
    """
    file_path = os.path.join(SAVE_DIR, f"autism_all_{year}.json")
    
    # Skip if the file already exists (Resume capability)
    if os.path.exists(file_path):
        print(f"-> [{year}] File already exists. Skipping...")
        return

    print(f"\n>>> Processing Year: {year}...")
    
    # STEP 1: Execute Search and store results on NCBI's server (usehistory='y')
    search_term = f"{EXTENDED_QUERY} AND {year}[PDAT]"
    try:
        search_handle = Entrez.esearch(db="pubmed", term=search_term, usehistory="y")
        search_results = Entrez.read(search_handle)
        search_handle.close()
    except Exception as e:
        print(f"   [Error] Initial search failed for {year}: {e}")
        return

    count = int(search_results["Count"])
    webenv = search_results["WebEnv"]
    query_key = search_results["QueryKey"]
    
    print(f"   Total papers found: {count}. Starting batch download...")

    batch_size = 200 # Fetching 200 records per request
    year_data = []

    for start in range(0, count, batch_size):
        try:
            # STEP 2: Fetch details in batches
            fetch_handle = Entrez.efetch(
                db="pubmed",
                retstart=start,
                retmax=batch_size,
                webenv=webenv,
                query_key=query_key,
                retmode="xml"
            )
            records = Entrez.read(fetch_handle)
            fetch_handle.close()

            # STEP 3: Parse individual article metadata
            for article in records['PubmedArticle']:
                try:
                    medline = article['MedlineCitation']
                    article_data = medline['Article']
                    
                    # Extract Title
                    title = article_data.get('ArticleTitle', '')
                    
                    # Extract and merge Abstract segments
                    abstract = ""
                    if 'Abstract' in article_data:
                        abstract_list = article_data['Abstract'].get('AbstractText', [])
                        abstract = " ".join([str(text) for text in abstract_list])
                    
                    # Extract Keywords
                    keywords = []
                    if 'KeywordList' in medline:
                        for kw_list in medline['KeywordList']:
                            keywords.extend([str(kw) for kw in kw_list])

                    # Build Paper Object
                    paper = {
                        "pmid": str(medline['PMID']),
                        "title": title,
                        "abstract": abstract,
                        "keywords": keywords,
                        "journal": article_data['Journal'].get('Title', ''),
                        "pub_date": f"{year}",
                        "doi": next((str(id) for id in article['PubmedData']['ArticleIdList'] if id.attributes.get('IdType') == 'doi'), None)
                    }
                    year_data.append(paper)
                except Exception:
                    # Skip problematic individual records to continue the loop
                    continue
            
            print(f"   Progress: {start + len(records['PubmedArticle'])}/{count}", end='\r')
            
            # Compliance with NCBI Rate Limits: 10 requests per second with API Key
            time.sleep(0.1)

        except Exception as e:
            print(f"\n   [Warning] Error at batch {start} for year {year}: {e}")
            time.sleep(3) # Wait before retry

    # STEP 4: Save the list to a JSON file
    if year_data:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(year_data, f, ensure_ascii=False, indent=2)
        print(f"\n   Saved {len(year_data)} records for {year}.")
    else:
        print(f"\n   No records found for {year}.")


if __name__ == "__main__":
    
    y = 2024
    download_data_for_year(y)
    
    '''
    start_year = 1943
    current_year = 2024
    
    print(f"Initializing global download for Autism research data...")
    for y in range(start_year, current_year + 1):
        download_data_for_year(y)
    
    print("\nDownload complete. All yearly JSON files are in the folder:", SAVE_DIR)
    '''