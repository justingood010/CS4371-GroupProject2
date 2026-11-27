import os
import pandas as pd
import json
from openai import OpenAI
from dotenv import load_dotenv
import re
from datetime import datetime

# 1. SETUP & CONFIG
load_dotenv()
PROCESSED_DATA = "data/processed/neighbourhood_month.parquet"
OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

groq_client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=os.getenv("GROQ_API_KEY")
)

# 2. DATA PROCESSING FUNCTIONS
def calculate_monthly_metrics(df, month_int):
    """Calculates average incidents, city share, and seasonal change."""
    
    # 1. Get a master list of all community areas to ensure alignment
    all_areas = df['community_area'].unique()
    
    # 2. LEVEL: Filter for the chosen month
    df_filtered = df[df['month'] == month_int]
    city_total_month = df_filtered['total_incidents'].sum()
    
    # Calculate metrics and REINDEX to the master list
    chosen_month_avg = df_filtered.groupby('community_area')['total_incidents'].mean().reindex(all_areas)
    city_share = (df_filtered.groupby('community_area')['total_incidents'].sum() / city_total_month * 100).reindex(all_areas)
    
    # 3. SEASONAL CHANGE
    overall_avg = df.groupby('community_area')['total_incidents'].mean().reindex(all_areas)
    seasonal_change = chosen_month_avg / overall_avg

    # 4. PERSON/PROPERTY SPLIT
    persons_avg = df_filtered.groupby('community_area')['incidents_persons'].mean().reindex(all_areas)
    property_avg = df_filtered.groupby('community_area')['incidents_property'].mean().reindex(all_areas)
    
    person_share = (persons_avg / chosen_month_avg) * 100
    property_share = (property_avg / chosen_month_avg) * 100

    # Combine into a single DataFrame
    result_df = pd.DataFrame({
        'community_area': all_areas,
        'avg_incidents': chosen_month_avg.values,
        'city_share_pct': city_share.values,
        'seasonal_change_ratio': seasonal_change.values,
        'person_share_pct': person_share.values,
        'property_share_pct': property_share.values
    })
    
    # Fill any NaNs that might have occurred if a neighborhood had 0 crimes in July
    result_df.fillna(0, inplace=True)
    
    return result_df

def get_top_and_bottom_lists(df_metrics):
    """Extracts the 5 highest, 5 largest seasonal increases, and 5 lowest."""
    cols_incidents = ['community_area', 'avg_incidents', 'person_share_pct', 'property_share_pct']
    cols_seasonal = ['community_area', 'seasonal_change_ratio', 'person_share_pct', 'property_share_pct']

    top5 = df_metrics.nlargest(5, 'avg_incidents')[cols_incidents].copy()
    top5['list_reason'] = 'Highest Incidents'
    
    bottom5 = df_metrics.nsmallest(5, 'avg_incidents')[cols_incidents].copy()
    bottom5['list_reason'] = 'Fewest Incidents'
    
    seasonal5 = df_metrics.nlargest(5, 'seasonal_change_ratio')[cols_seasonal].copy()
    seasonal5['list_reason'] = 'Largest Seasonal Increase'

    # Combine and reorder columns for final output
    combined = pd.concat([top5, bottom5, seasonal5], ignore_index=True)
    combined = combined[['list_reason', 'community_area', 'avg_incidents', 'seasonal_change_ratio', 'person_share_pct', 'property_share_pct']]
    combined = combined.astype(str).replace('nan', 'N/A')
    combined = combined.fillna("N/A")
    
    return combined

# 3. LLM & VALIDATION FUNCTIONS
def generate_llm_summary(rows_dict, month_name):
    """Sends the 15 rows to the LLM and returns the summary text."""

    data_string = json.dumps(rows_dict, indent=2)

    prompt = f"""
    You are a helpful data analyst. I am providing you with exactly 15 rows of neighborhood crime data for {month_name}.
    Note: "person_share_pct" and "property_share_pct" represent the percentage of total incidents in that neighborhood.
    
    Here is the data:
    {data_string}
    
    Task: Write a short, traveler-friendly summary of these findings. 
    CRITICAL RULE: You must ONLY use the exact numbers provided in the JSON above. Do not calculate new numbers, do not round numbers, and do not use numbers that are not explicitly in the text.
    """
    try:
        response = groq_client.chat.completions.create(
            model="openai/gpt-oss-120b",
            messages=[
                {"role": "system", "content": "You are a factual data summarizer. You strictly adhere to provided data."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1, 
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error calling Groq API: {e}")
        return None

def verify_llm_numbers(summary_text, rows_dict):
    """Checks that every number in the summary exists in the raw JSON."""
    if not summary_text:
        return False
        
    # Extract all numbers from the summary text
    summary_numbers = re.findall(r"[-+]?\d*\.\d+|\d+", summary_text)
    
    # Extract all numbers from the actual JSON data
    data_string = json.dumps(rows_dict)
    json_numbers = re.findall(r"[-+]?\d*\.\d+|\d+", data_string)
    
    # Check if every number in the summary exists in the JSON numbers
    for num in summary_numbers:
        if num not in json_numbers:
            print(f"Validation Failed: LLM invented the number '{num}'")
            return False
    return True

# 4. MAIN EXECUTION
def main(month_name="July"):
    print(f"Processing data for {month_name}...")
    
    # Convert month name to integer (e.g., "July" -> 7)
    month_int = datetime.strptime(month_name, "%B").month
    
    df = pd.read_parquet(PROCESSED_DATA)
    metrics_df = calculate_monthly_metrics(df, month_int)
    
    # Get the combined dataframe of 15 rows
    combined_df = get_top_and_bottom_lists(metrics_df)
    
    # Save CSV Output
    output_csv = f"{OUTPUT_DIR}/t2_outlook_{month_name.lower()}.csv"
    combined_df.to_csv(output_csv, index=False)
    print(f"Saved {output_csv}")
    
    # Convert to list of dictionaries for both LLM and Validation
    llm_input_data = combined_df.to_dict(orient='records')
    
    max_retries = 3
    for attempt in range(max_retries):
        summary = generate_llm_summary(llm_input_data, month_name)
        
        if verify_llm_numbers(summary, llm_input_data):
            output_md = f"{OUTPUT_DIR}/t2_summary_{month_name.lower()}.md"
            with open(output_md, "w", encoding="utf-8") as f:
                f.write(summary)
            print("LLM Summary verified and saved!")
            break
        else:
            print(f"LLM hallucinated a number. Retrying {attempt+1}/{max_retries}...")
    else:
        print("LLM failed verification. Showing the table alone.")
        output_md = f"{OUTPUT_DIR}/t2_summary_{month_name.lower()}.md"
        with open(output_md, "w", encoding="utf-8") as f:
            f.write(combined_df.to_markdown(index=False))

if __name__ == "__main__":
    main("July")
    main("December")