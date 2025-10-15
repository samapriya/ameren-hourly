import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
import time
import pytz
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import random

class AmerenRateFetcher:
    def __init__(self, output_file='ameren_rates.json', concurrent_requests=10, max_retries=3):
        self.output_file = output_file
        self.concurrent_requests = concurrent_requests
        self.max_retries = max_retries
        self.base_url = 'https://www.ameren.com/api/ameren/promotion/RtpHourlyPricesbyDate'
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://www.ameren.com',
            'referer': 'https://www.ameren.com/bill/rates/power-smart-pricing/prices',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
    def load_existing_data(self):
        """Load existing JSON data or create new structure"""
        if Path(self.output_file).exists():
            with open(self.output_file, 'r') as f:
                return json.load(f)
        return {'rates': [], 'last_updated': None}
    
    def save_data(self, data):
        """Save data to JSON file"""
        with open(self.output_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def fetch_rate_for_date(self, date_str):
        """Fetch rate data for a specific date"""
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={'SelectedDate': date_str},
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {date_str}: {e}")
            return None
    
    def date_exists(self, data, date_str):
        """Check if date already exists in data"""
        return any(entry.get('date') == date_str for entry in data['rates'])
    
    async def fetch_rate_async(self, session, date_str, semaphore):
        """Async fetch with retry logic and exponential backoff"""
        async with semaphore:
            for attempt in range(self.max_retries):
                try:
                    async with session.post(
                        self.base_url,
                        headers=self.headers,
                        json={'SelectedDate': date_str},
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            # Extract only hourlyPriceDetails, ignore chartBytes
                            hourly_data = data.get('hourlyPriceDetails', [])
                            return date_str, hourly_data, None
                        elif response.status == 429:  # Rate limited
                            wait_time = (2 ** attempt) + random.uniform(0, 1)
                            print(f"Rate limited on {date_str}, waiting {wait_time:.1f}s...")
                            await asyncio.sleep(wait_time)
                        else:
                            return date_str, None, f"HTTP {response.status}"
                except asyncio.TimeoutError:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    print(f"Timeout on {date_str}, retry {attempt + 1}/{self.max_retries}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(wait_time)
                except Exception as e:
                    return date_str, None, str(e)
            
            return date_str, None, "Max retries exceeded"
    
    async def fetch_batch_async(self, dates_to_fetch):
        """Fetch multiple dates concurrently"""
        semaphore = asyncio.Semaphore(self.concurrent_requests)
        
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch_rate_async(session, date_str, semaphore)
                for date_str in dates_to_fetch
            ]
            
            results = []
            completed = 0
            total = len(tasks)
            
            for coro in asyncio.as_completed(tasks):
                result = await coro
                results.append(result)
                completed += 1
                
                date_str, data, error = result
                if data:
                    print(f"✓ {date_str} ({completed}/{total})")
                else:
                    print(f"✗ {date_str} - {error} ({completed}/{total})")
            
            return results
    
    def backfill_historical_data(self, start_date='2015-01-01'):
        """Fetch all historical data from start_date to today using concurrent requests"""
        print(f"Starting historical data backfill with {self.concurrent_requests} concurrent requests...")
        data = self.load_existing_data()
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.now()
        
        # Generate list of all dates
        all_dates = []
        current = start
        while current <= end:
            all_dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        # Filter out dates that already exist
        dates_to_fetch = [
            date_str for date_str in all_dates 
            if not self.date_exists(data, date_str)
        ]
        
        if not dates_to_fetch:
            print("All dates already fetched!")
            return data
        
        print(f"Need to fetch {len(dates_to_fetch)} dates (skipping {len(all_dates) - len(dates_to_fetch)} existing)")
        
        # Process in batches for periodic saving
        batch_size = 100
        total_fetched = 0
        total_failed = 0
        
        for i in range(0, len(dates_to_fetch), batch_size):
            batch = dates_to_fetch[i:i + batch_size]
            print(f"\n--- Batch {i//batch_size + 1} ({len(batch)} dates) ---")
            
            # Run async batch
            results = asyncio.run(self.fetch_batch_async(batch))
            
            # Process results
            for date_str, rate_data, error in results:
                if rate_data:
                    entry = {
                        'date': date_str,
                        'data': rate_data,
                        'fetched_at': datetime.now().isoformat()
                    }
                    data['rates'].append(entry)
                    total_fetched += 1
                else:
                    total_failed += 1
            
            # Save progress after each batch
            data['last_updated'] = datetime.now().isoformat()
            self.save_data(data)
            print(f"Saved progress: {total_fetched} successful, {total_failed} failed")
        
        print(f"\nBackfill complete! Total fetched: {total_fetched}, Total failed: {total_failed}")
        return data
    
    def fetch_next_day_rate(self):
        """Fetch tomorrow's rate (available after 6 PM Central)"""
        central = pytz.timezone('US/Central')
        now_central = datetime.now(central)
        
        # Check if it's after 6 PM Central
        if now_central.hour < 18:
            print(f"Tomorrow's rates not yet available. Check after 6 PM Central (currently {now_central.strftime('%I:%M %p')})")
            return None
        
        tomorrow = (now_central + timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"Fetching rate for {tomorrow}...")
        
        data = self.load_existing_data()
        
        if self.date_exists(data, tomorrow):
            print(f"Rate for {tomorrow} already exists")
            return data
        
        rate_data = self.fetch_rate_for_date(tomorrow)
        
        if rate_data:
            entry = {
                'date': tomorrow,
                'data': rate_data,
                'fetched_at': datetime.now().isoformat()
            }
            data['rates'].append(entry)
            data['last_updated'] = datetime.now().isoformat()
            self.save_data(data)
            print(f"Successfully saved rate for {tomorrow}")
        
        return data

if __name__ == "__main__":
    fetcher = AmerenRateFetcher(
        output_file='ameren_rates.json',
        concurrent_requests=10,
        max_retries=3
    )
    
    fetcher.backfill_historical_data('2015-01-01')
    
    fetcher.fetch_next_day_rate()
