import requests
import pandas as pd
import sqlite3
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import numpy as np
from dataclasses import dataclass, asdict
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class PhysicianProfile:
    """Data structure for physician information"""
    npi: str
    first_name: str
    last_name: str
    organization_name: str
    street_address_1: str
    city: str
    state: str
    zip_code: str
    country: str
    specialty_description: str
    specialty_code: str
    medicare_participation: str
    
@dataclass
class ProcedureData:
    """Data structure for procedure pricing data"""
    npi: str
    physician_name: str
    year: int
    hcpcs_code: str
    hcpcs_description: str
    line_service_count: int
    beneficiary_unique_count: int
    average_submitted_charge: float
    average_medicare_allowed: float
    average_medicare_payment: float
    average_medicare_standard_payment: float

class CMSOrthopedicCollector:
    """Collector for CMS orthopedic physician data in Atlanta"""
    
    def __init__(self, db_path: str = "cms_orthopedic_data.db"):
        self.db_path = db_path
        self.session = requests.Session()
        self.base_url = "https://data.cms.gov/provider-data/api/1/datastore/query"
        
        # CMS dataset resource IDs for different years
        self.resource_ids = {
            2022: "medicare-physician-other-practitioners-by-provider-and-service-2022",
            2021: "medicare-physician-other-practitioners-by-provider-and-service-2021", 
            2020: "medicare-physician-other-practitioners-by-provider-and-service-2020",
            2019: "medicare-physician-other-practitioners-by-provider-and-service-2019",
            2018: "medicare-physician-other-practitioners-by-provider-and-service-2018"
        }
        
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database for CMS data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Physicians table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS physicians (
                npi TEXT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                organization_name TEXT,
                street_address_1 TEXT,
                city TEXT,
                state TEXT,
                zip_code TEXT,
                country TEXT,
                specialty_description TEXT,
                specialty_code TEXT,
                medicare_participation TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Procedure data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS procedure_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                npi TEXT,
                physician_name TEXT,
                year INTEGER,
                hcpcs_code TEXT,
                hcpcs_description TEXT,
                line_service_count INTEGER,
                beneficiary_unique_count INTEGER,
                average_submitted_charge REAL,
                average_medicare_allowed REAL,
                average_medicare_payment REAL,
                average_medicare_standard_payment REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (npi) REFERENCES physicians (npi)
            )
        ''')
        
        # Collection logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER,
                status TEXT,
                records_collected INTEGER DEFAULT 0,
                physicians_found INTEGER DEFAULT 0,
                error_message TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    
    def search_atlanta_orthopedic_physicians(self, year: int, limit: int = 1000) -> List[Dict]:
        """Search for orthopedic physicians in Atlanta metro area"""
        logger.info(f"Searching for Atlanta orthopedic physicians for year {year}")
        
        resource_id = self.resource_ids.get(year)
        if not resource_id:
            logger.error(f"No resource ID found for year {year}")
            return []
        
        # Atlanta metro area ZIP codes (sample of major ones)
        atlanta_zips = [
            "30309", "30324", "30326", "30327", "30305", "30306", "30307", "30308",
            "30309", "30310", "30311", "30312", "30313", "30314", "30315", "30316",
            "30317", "30318", "30319", "30324", "30325", "30326", "30327", "30328",
            "30329", "30331", "30332", "30334", "30336", "30337", "30338", "30339",
            "30340", "30341", "30342", "30344", "30345", "30346", "30347", "30348",
            "30349", "30350", "30354", "30360", "30361", "30363", "30364", "30366",
            "30368", "30369", "30370", "30371", "30374", "30375", "30376", "30377",
            "30378", "30380", "30384", "30385", "30388", "30392", "30394", "30396",
            "30398"
        ]
        
        all_records = []
        
        # Search for orthopedic-related specialties
        orthopedic_specialties = [
            "Orthopedic Surgery",
            "Orthopaedic Surgery", 
            "Hand Surgery",
            "Sports Medicine",
            "Interventional Pain Management"
        ]
        
        try:
            # Build query to find orthopedic physicians in Atlanta area
            # Note: CMS API has limitations, so we'll do broader searches and filter
            
            params = {
                'resource_id': resource_id,
                'limit': limit,
                'offset': 0,
                'filters': {
                    'Rndrng_Prvdr_State_Abrvtn': 'GA'  # Georgia state filter
                }
            }
            
            response = self.session.get(
                self.base_url,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                for record in data.get('records', []):
                    # Filter for Atlanta area and orthopedic specialties
                    zip_code = record.get('Rndrng_Prvdr_Zip5', '')
                    specialty = record.get('Provider_Type', '')
                    city = record.get('Rndrng_Prvdr_City', '').upper()
                    
                    # Check if in Atlanta metro or has orthopedic specialty
                    is_atlanta_area = (
                        zip_code in atlanta_zips or 
                        'ATLANTA' in city or
                        'DECATUR' in city or
                        'MARIETTA' in city or
                        'ROSWELL' in city or
                        'ALPHARETTA' in city
                    )
                    
                    is_orthopedic = any(ortho.upper() in specialty.upper() for ortho in orthopedic_specialties)
                    
                    if is_atlanta_area and is_orthopedic:
                        all_records.append(record)
                
                logger.info(f"Found {len(all_records)} potential orthopedic records for {year}")
            
            else:
                logger.error(f"API request failed with status {response.status_code}: {response.text}")
        
        except Exception as e:
            logger.error(f"Error searching physicians for {year}: {e}")
        
        return all_records
    
    def extract_physician_profile(self, record: Dict) -> PhysicianProfile:
        """Extract physician profile from CMS record"""
        return PhysicianProfile(
            npi=record.get('Rndrng_NPI', ''),
            first_name=record.get('Rndrng_Prvdr_First_Name', ''),
            last_name=record.get('Rndrng_Prvdr_Last_Name', ''),
            organization_name=record.get('Rndrng_Prvdr_Org_Name', ''),
            street_address_1=record.get('Rndrng_Prvdr_St1', ''),
            city=record.get('Rndrng_Prvdr_City', ''),
            state=record.get('Rndrng_Prvdr_State_Abrvtn', ''),
            zip_code=record.get('Rndrng_Prvdr_Zip5', ''),
            country=record.get('Rndrng_Prvdr_Cntry', ''),
            specialty_description=record.get('Provider_Type', ''),
            specialty_code=record.get('Medicare_Participation_Indicator', ''),
            medicare_participation=record.get('Medicare_Participation_Indicator', '')
        )
    
    def extract_procedure_data(self, record: Dict, year: int) -> ProcedureData:
        """Extract procedure data from CMS record"""
        physician_name = f"{record.get('Rndrng_Prvdr_First_Name', '')} {record.get('Rndrng_Prvdr_Last_Name', '')}"
        
        return ProcedureData(
            npi=record.get('Rndrng_NPI', ''),
            physician_name=physician_name.strip(),
            year=year,
            hcpcs_code=record.get('HCPCS_Cd', ''),
            hcpcs_description=record.get('HCPCS_Desc', ''),
            line_service_count=int(record.get('Tot_Srvcs', 0) or 0),
            beneficiary_unique_count=int(record.get('Tot_Benes', 0) or 0),
            average_submitted_charge=float(record.get('Avg_Sbmtd_Chrg', 0) or 0),
            average_medicare_allowed=float(record.get('Avg_Mdcr_Alowd_Amt', 0) or 0),
            average_medicare_payment=float(record.get('Avg_Mdcr_Pymt_Amt', 0) or 0),
            average_medicare_standard_payment=float(record.get('Avg_Mdcr_Stdzd_Amt', 0) or 0)
        )
    
    def save_physician_data(self, physicians: List[PhysicianProfile]):
        """Save physician profiles to database"""
        if not physicians:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for physician in physicians:
            # Use INSERT OR REPLACE to handle duplicates
            cursor.execute('''
                INSERT OR REPLACE INTO physicians 
                (npi, first_name, last_name, organization_name, street_address_1, 
                 city, state, zip_code, country, specialty_description, 
                 specialty_code, medicare_participation)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                physician.npi, physician.first_name, physician.last_name,
                physician.organization_name, physician.street_address_1,
                physician.city, physician.state, physician.zip_code,
                physician.country, physician.specialty_description,
                physician.specialty_code, physician.medicare_participation
            ))
        
        conn.commit()
        conn.close()
        logger.info(f"Saved {len(physicians)} physician profiles")
    
    def save_procedure_data(self, procedures: List[ProcedureData]):
        """Save procedure data to database"""
        if not procedures:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for procedure in procedures:
            cursor.execute('''
                INSERT INTO procedure_data 
                (npi, physician_name, year, hcpcs_code, hcpcs_description,
                 line_service_count, beneficiary_unique_count, average_submitted_charge,
                 average_medicare_allowed, average_medicare_payment, average_medicare_standard_payment)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                procedure.npi, procedure.physician_name, procedure.year,
                procedure.hcpcs_code, procedure.hcpcs_description,
                procedure.line_service_count, procedure.beneficiary_unique_count,
                procedure.average_submitted_charge, procedure.average_medicare_allowed,
                procedure.average_medicare_payment, procedure.average_medicare_standard_payment
            ))
        
        conn.commit()
        conn.close()
        logger.info(f"Saved {len(procedures)} procedure records")
    
    def log_collection_run(self, year: int, status: str, records: int = 0, physicians: int = 0, error: str = None):
        """Log collection run results"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO collection_logs (year, status, records_collected, physicians_found, error_message)
            VALUES (?, ?, ?, ?, ?)
        ''', (year, status, records, physicians, error))
        
        conn.commit()
        conn.close()
    
    def collect_year_data(self, year: int) -> Tuple[int, int]:
        """Collect data for a specific year"""
        logger.info(f"Collecting CMS data for year {year}")
        
        try:
            # Search for orthopedic physicians
            records = self.search_atlanta_orthopedic_physicians(year, limit=2000)
            
            if not records:
                logger.warning(f"No records found for {year}")
                self.log_collection_run(year, "NO_DATA", 0, 0)
                return 0, 0
            
            # Process records to extract physician profiles and procedure data
            physicians_dict = {}  # Use dict to deduplicate by NPI
            procedures = []
            
            for record in records:
                # Extract physician profile
                physician = self.extract_physician_profile(record)
                if physician.npi:  # Only add if we have an NPI
                    physicians_dict[physician.npi] = physician
                
                # Extract procedure data
                procedure = self.extract_procedure_data(record, year)
                if procedure.npi and procedure.hcpcs_code:  # Only add if we have valid data
                    procedures.append(procedure)
            
            # Convert dict to list
            physicians = list(physicians_dict.values())
            
            # Save to database
            self.save_physician_data(physicians)
            self.save_procedure_data(procedures)
            
            # Log success
            self.log_collection_run(year, "SUCCESS", len(procedures), len(physicians))
            
            logger.info(f"Year {year}: Collected {len(physicians)} physicians, {len(procedures)} procedures")
            return len(physicians), len(procedures)
        
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Collection failed for {year}: {error_msg}")
            self.log_collection_run(year, "FAILED", 0, 0, error_msg)
            return 0, 0
    
    def run_full_collection(self, years: List[int] = None) -> Dict[str, int]:
        """Run collection for multiple years"""
        if years is None:
            years = [2022, 2021, 2020, 2019, 2018]  # Last 5 years available
        
        logger.info(f"Starting CMS data collection for years: {years}")
        
        total_physicians = 0
        total_procedures = 0
        results = {}
        
        for year in years:
            try:
                physicians, procedures = self.collect_year_data(year)
                total_physicians += physicians
                total_procedures += procedures
                results[year] = {'physicians': physicians, 'procedures': procedures}
                
                # Rate limiting - be respectful to CMS API
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Failed to collect data for {year}: {e}")
                results[year] = {'physicians': 0, 'procedures': 0, 'error': str(e)}
        
        logger.info(f"Collection complete. Total: {total_physicians} physicians, {total_procedures} procedures")
        return results
    
    def get_collection_summary(self) -> Dict:
        """Get summary of collected data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Count physicians
        cursor.execute("SELECT COUNT(DISTINCT npi) FROM physicians")
        total_physicians = cursor.fetchone()[0]
        
        # Count procedures by year
        cursor.execute("""
            SELECT year, COUNT(*) as procedure_count, COUNT(DISTINCT npi) as physician_count
            FROM procedure_data 
            GROUP BY year 
            ORDER BY year DESC
        """)
        year_summary = cursor.fetchall()
        
        # Top procedures
        cursor.execute("""
            SELECT hcpcs_code, hcpcs_description, COUNT(*) as frequency,
                   AVG(average_submitted_charge) as avg_submitted,
                   AVG(average_medicare_allowed) as avg_allowed,
                   AVG(average_medicare_payment) as avg_payment
            FROM procedure_data 
            GROUP BY hcpcs_code, hcpcs_description
            ORDER BY frequency DESC
            LIMIT 10
        """)
        top_procedures = cursor.fetchall()
        
        # Collection logs
        cursor.execute("""
            SELECT year, status, records_collected, physicians_found, timestamp
            FROM collection_logs
            ORDER BY timestamp DESC
            LIMIT 10
        """)
        recent_logs = cursor.fetchall()
        
        conn.close()
        
        return {
            'total_physicians': total_physicians,
            'year_summary': year_summary,
            'top_procedures': top_procedures,
            'recent_logs': recent_logs
        }

class CMSDataAnalyzer:
    """Analyzer for CMS orthopedic data"""
    
    def __init__(self, db_path: str = "cms_orthopedic_data.db"):
        self.db_path = db_path
    
    def get_physician_summary(self) -> pd.DataFrame:
        """Get summary of all physicians"""
        conn = sqlite3.connect(self.db_path)
        query = """
        SELECT 
            p.npi,
            p.first_name || ' ' || p.last_name as physician_name,
            p.specialty_description,
            p.city,
            p.zip_code,
            COUNT(pd.id) as total_procedures,
            COUNT(DISTINCT pd.hcpcs_code) as unique_procedure_types,
            ROUND(AVG(pd.average_submitted_charge), 2) as avg_submitted_charge,
            ROUND(AVG(pd.average_medicare_allowed), 2) as avg_medicare_allowed,
            ROUND(AVG(pd.average_medicare_payment), 2) as avg_medicare_payment
        FROM physicians p
        LEFT JOIN procedure_data pd ON p.npi = pd.npi
        GROUP BY p.npi, p.first_name, p.last_name, p.specialty_description, p.city, p.zip_code
        ORDER BY total_procedures DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    
    def get_procedure_comparison(self, procedure_code: str = None) -> pd.DataFrame:
        """Compare procedures across physicians"""
        conn = sqlite3.connect(self.db_path)
        
        if procedure_code:
            query = """
            SELECT 
                pd.physician_name,
                pd.hcpcs_code,
                pd.hcpcs_description,
                pd.year,
                pd.line_service_count,
                pd.average_submitted_charge,
                pd.average_medicare_allowed,
                pd.average_medicare_payment,
                p.city,
                p.zip_code
            FROM procedure_data pd
            JOIN physicians p ON pd.npi = p.npi
            WHERE pd.hcpcs_code = ?
            ORDER BY pd.average_medicare_payment ASC
            """
            df = pd.read_sql_query(query, conn, params=[procedure_code])
        else:
            query = """
            SELECT 
                pd.hcpcs_code,
                pd.hcpcs_description,
                COUNT(*) as frequency,
                COUNT(DISTINCT pd.npi) as physician_count,
                ROUND(AVG(pd.average_submitted_charge), 2) as avg_submitted_charge,
                ROUND(AVG(pd.average_medicare_allowed), 2) as avg_medicare_allowed,
                ROUND(AVG(pd.average_medicare_payment), 2) as avg_medicare_payment,
                ROUND(MIN(pd.average_medicare_payment), 2) as min_payment,
                ROUND(MAX(pd.average_medicare_payment), 2) as max_payment
            FROM procedure_data pd
            JOIN physicians p ON pd.npi = p.npi
            GROUP BY pd.hcpcs_code, pd.hcpcs_description
            ORDER BY frequency DESC
            LIMIT 20
            """
            df = pd.read_sql_query(query, conn)
        
        conn.close()
        return df
    
    def get_price_trends(self, procedure_code: str) -> pd.DataFrame:
        """Get price trends over time for a procedure"""
        conn = sqlite3.connect(self.db_path)
        query = """
        SELECT 
            year,
            COUNT(*) as procedure_count,
            ROUND(AVG(average_submitted_charge), 2) as avg_submitted,
            ROUND(AVG(average_medicare_allowed), 2) as avg_allowed,
            ROUND(AVG(average_medicare_payment), 2) as avg_payment
        FROM procedure_data
        WHERE hcpcs_code = ?
        GROUP BY year
        ORDER BY year
        """
        df = pd.read_sql_query(query, conn, params=[procedure_code])
        conn.close()
        return df

def main():
    """Main function to run the CMS collection prototype"""
    print("🏥 CMS Orthopedic Data Collector - Atlanta Prototype")
    print("=" * 60)
    
    # Initialize collector
    collector = CMSOrthopedicCollector()
    
    # Run collection (start with just 2022 for testing)
    print("\n📊 Starting data collection...")
    results = collector.run_full_collection([2022, 2021])  # Start with 2 years
    
    # Print results
    print("\n📈 Collection Results:")
    for year, data in results.items():
        if 'error' in data:
            print(f"  {year}: ERROR - {data['error']}")
        else:
            print(f"  {year}: {data['physicians']} physicians, {data['procedures']} procedures")
    
    # Get and display summary
    print("\n📋 Data Summary:")
    summary = collector.get_collection_summary()
    print(f"  Total Physicians: {summary['total_physicians']}")
    
    if summary['year_summary']:
        print("  By Year:")
        for year_data in summary['year_summary']:
            year, proc_count, phys_count = year_data
            print(f"    {year}: {proc_count} procedures from {phys_count} physicians")
    
    # Analyze data if we have any
    if summary['total_physicians'] > 0:
        print("\n🔍 Running Analysis...")
        analyzer = CMSDataAnalyzer()
        
        # Physician summary
        physician_df = analyzer.get_physician_summary()
        print(f"\n👨‍⚕️ Top Physicians by Procedure Volume:")
        print(physician_df.head().to_string())
        
        # Procedure comparison
        procedure_df = analyzer.get_procedure_comparison()
        print(f"\n⚕️ Most Common Procedures:")
        print(procedure_df.head().to_string())
    
    print("\n✅ Prototype collection complete!")
    print(f"📁 Data saved to: {collector.db_path}")

if __name__ == "__main__":
    main()