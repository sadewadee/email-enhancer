"""
Post-Processor Module for Email Scraper & Validator
Handles merging and formatting of extracted contact data into wide-form output.
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Any, Union
import os
from datetime import datetime
import json


class PostProcessor:
    """
    Handles post-processing of extracted contact data including merging,
    deduplication, and formatting into wide-form output.
    """
    
    def __init__(self):
        """Initialize post-processor."""
        # Setup module logger - rely on root configuration
        self.logger = logging.getLogger(__name__)
    
    def merge_csv_files(self, 
                       input_files: List[str], 
                       output_file: str,
                       merge_strategy: str = 'union') -> Dict[str, Any]:
        """
        Merge multiple processed CSV files into a single file.
        
        Args:
            input_files: List of CSV file paths to merge
            output_file: Path for merged output file
            merge_strategy: 'union' (all columns) or 'intersection' (common columns)
            
        Returns:
            Dictionary with merge statistics
        """
        try:
            dataframes = []
            total_rows = 0
            
            # Read all CSV files
            for file_path in input_files:
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    df['source_file'] = os.path.basename(file_path)
                    dataframes.append(df)
                    total_rows += len(df)
                    # Simplified logging - just load without details
                else:
                    self.logger.warning(f"File not found: {file_path}")
            
            if not dataframes:
                raise ValueError("No valid CSV files found to merge")
            
            # Merge dataframes
            if merge_strategy == 'union':
                merged_df = pd.concat(dataframes, ignore_index=True, sort=False)
            else:  # intersection
                # Find common columns
                common_columns = set(dataframes[0].columns)
                for df in dataframes[1:]:
                    common_columns = common_columns.intersection(set(df.columns))
                
                # Keep only common columns
                filtered_dfs = [df[list(common_columns)] for df in dataframes]
                merged_df = pd.concat(filtered_dfs, ignore_index=True)
            
            # Save merged file
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            merged_df.to_csv(output_file, index=False)
            
            stats = {
                'input_files': len(input_files),
                'total_input_rows': total_rows,
                'merged_rows': len(merged_df),
                'columns': len(merged_df.columns),
                'merge_strategy': merge_strategy,
                'output_file': output_file
            }
            
            self.logger.info(f"📋 Merged {len(input_files)} files → {os.path.basename(output_file)}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error merging CSV files: {str(e)}")
            raise
    
    def create_wide_form_output(self, 
                               input_file: str, 
                               output_file: str,
                               max_contacts_per_type: int = 10) -> Dict[str, Any]:
        """
        Convert contact data to wide-form format with separate columns for each contact.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            max_contacts_per_type: Maximum number of contacts per type to include
            
        Returns:
            Dictionary with conversion statistics
        """
        try:
            # Read input CSV
            df = pd.read_csv(input_file)
            
            # Create wide-form DataFrame
            wide_df = df.copy()
            
            # Process emails
            wide_df = self._expand_contacts_to_columns(
                wide_df, 'emails', 'email', max_contacts_per_type
            )
            
            # Process validated emails
            wide_df = self._expand_validated_emails_to_columns(
                wide_df, 'validated_emails', 'validated_email', max_contacts_per_type
            )
            
            # Process phone numbers
            wide_df = self._expand_contacts_to_columns(
                wide_df, 'phones', 'phone', max_contacts_per_type
            )
            
            # Process WhatsApp contacts
            wide_df = self._expand_contacts_to_columns(
                wide_df, 'whatsapp', 'whatsapp', max_contacts_per_type
            )
            
            # Add summary columns
            wide_df = self._add_summary_columns(wide_df)
            
            # Save wide-form output
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            wide_df.to_csv(output_file, index=False)
            
            stats = {
                'input_rows': len(df),
                'output_rows': len(wide_df),
                'input_columns': len(df.columns),
                'output_columns': len(wide_df.columns),
                'max_contacts_per_type': max_contacts_per_type
            }
            
            self.logger.info(f"📊 Wide-form created: {os.path.basename(output_file)}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error creating wide-form output: {str(e)}")
            raise
    
    def _expand_contacts_to_columns(self, 
                                   df: pd.DataFrame, 
                                   source_column: str, 
                                   prefix: str, 
                                   max_count: int) -> pd.DataFrame:
        """
        Expand semicolon-separated contacts into individual columns.
        
        Args:
            df: Input DataFrame
            source_column: Column containing semicolon-separated contacts
            prefix: Prefix for new columns
            max_count: Maximum number of columns to create
            
        Returns:
            DataFrame with expanded columns
        """
        if source_column not in df.columns:
            return df
        
        # Create new columns
        for i in range(1, max_count + 1):
            df[f'{prefix}_{i}'] = ''
        
        # Fill columns with contact data
        for idx, row in df.iterrows():
            contacts_str = str(row[source_column]) if pd.notna(row[source_column]) else ''
            if contacts_str and contacts_str != 'nan':
                contacts = [c.strip() for c in contacts_str.split(';') if c.strip()]
                
                for i, contact in enumerate(contacts[:max_count]):
                    df.at[idx, f'{prefix}_{i+1}'] = contact
        
        return df
    
    def _expand_validated_emails_to_columns(self, 
                                          df: pd.DataFrame, 
                                          source_column: str, 
                                          prefix: str, 
                                          max_count: int) -> pd.DataFrame:
        """
        Expand validated emails with status into individual columns.
        
        Args:
            df: Input DataFrame
            source_column: Column containing validated emails with status
            prefix: Prefix for new columns
            max_count: Maximum number of columns to create
            
        Returns:
            DataFrame with expanded columns
        """
        if source_column not in df.columns:
            return df
        
        # Create new columns for emails and their statuses
        for i in range(1, max_count + 1):
            df[f'{prefix}_{i}'] = ''
            df[f'{prefix}_{i}_status'] = ''
        
        # Fill columns with validated email data
        for idx, row in df.iterrows():
            validated_str = str(row[source_column]) if pd.notna(row[source_column]) else ''
            if validated_str and validated_str != 'nan':
                validated_emails = [v.strip() for v in validated_str.split(';') if v.strip()]
                
                for i, validated_email in enumerate(validated_emails[:max_count]):
                    # Parse email and status from format "email (status)"
                    if '(' in validated_email and ')' in validated_email:
                        email_part = validated_email.split('(')[0].strip()
                        status_part = validated_email.split('(')[1].split(')')[0].strip()
                        
                        df.at[idx, f'{prefix}_{i+1}'] = email_part
                        df.at[idx, f'{prefix}_{i+1}_status'] = status_part
                    else:
                        df.at[idx, f'{prefix}_{i+1}'] = validated_email
                        df.at[idx, f'{prefix}_{i+1}_status'] = 'unknown'
        
        return df
    
    def _add_summary_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add summary columns with contact statistics.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with summary columns
        """
        # Count non-empty contacts
        email_columns = [col for col in df.columns if col.startswith('email_') and not col.endswith('_status')]
        phone_columns = [col for col in df.columns if col.startswith('phone_')]
        whatsapp_columns = [col for col in df.columns if col.startswith('whatsapp_')]
        validated_email_columns = [col for col in df.columns if col.startswith('validated_email_') and not col.endswith('_status')]
        
        # Count contacts
        df['total_emails_extracted'] = df[email_columns].apply(
            lambda row: sum(1 for val in row if val and str(val).strip()), axis=1
        )
        
        df['total_phones_extracted'] = df[phone_columns].apply(
            lambda row: sum(1 for val in row if val and str(val).strip()), axis=1
        )
        
        df['total_whatsapp_extracted'] = df[whatsapp_columns].apply(
            lambda row: sum(1 for val in row if val and str(val).strip()), axis=1
        )
        
        df['total_validated_emails'] = df[validated_email_columns].apply(
            lambda row: sum(1 for val in row if val and str(val).strip()), axis=1
        )
        
        # Add processing timestamp
        df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Calculate success indicators
        df['has_contacts'] = (
            (df['total_emails_extracted'] > 0) | 
            (df['total_phones_extracted'] > 0) | 
            (df['total_whatsapp_extracted'] > 0)
        )
        
        df['has_valid_emails'] = df['total_validated_emails'] > 0
        
        return df
    
    def deduplicate_contacts(self, 
                           input_file: str, 
                           output_file: str,
                           dedup_columns: List[str] = None) -> Dict[str, Any]:
        """
        Remove duplicate rows based on specified columns.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            dedup_columns: Columns to use for deduplication (default: ['url'])
            
        Returns:
            Dictionary with deduplication statistics
        """
        if dedup_columns is None:
            dedup_columns = ['url']
        
        try:
            # Read input CSV
            df = pd.read_csv(input_file)
            original_count = len(df)
            
            # Remove duplicates
            df_dedup = df.drop_duplicates(subset=dedup_columns, keep='first')
            final_count = len(df_dedup)
            
            # Save deduplicated file
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            df_dedup.to_csv(output_file, index=False)
            
            stats = {
                'original_rows': original_count,
                'deduplicated_rows': final_count,
                'duplicates_removed': original_count - final_count,
                'dedup_columns': dedup_columns
            }
            
            if stats['duplicates_removed'] > 0:
                self.logger.info(f"🧹 Removed {stats['duplicates_removed']} duplicates")
            # Don't log if no duplicates removed
            return stats
            
        except Exception as e:
            self.logger.error(f"Error deduplicating contacts: {str(e)}")
            raise
    
    def generate_summary_report(self, 
                              input_file: str, 
                              output_file: str) -> Dict[str, Any]:
        """
        Generate a summary report of the processing results.
        
        Args:
            input_file: Path to processed CSV file
            output_file: Path to summary report file
            
        Returns:
            Dictionary with summary statistics
        """
        try:
            # Read processed data
            df = pd.read_csv(input_file)
            
            # Calculate statistics
            stats = {
                'total_records': len(df),
                'successful_scrapes': len(df[df['scraping_status'] == 'success']),
                'failed_scrapes': len(df[df['scraping_status'] == 'failed']),
                'records_with_emails': len(df[df['emails_found'] > 0]),
                'records_with_phones': len(df[df['phones_found'] > 0]),
                'records_with_whatsapp': len(df[df['whatsapp_found'] > 0]),
                'records_with_validated_emails': len(df[df['validated_emails_count'] > 0]),
                'total_emails_found': df['emails_found'].sum(),
                'total_phones_found': df['phones_found'].sum(),
                'total_whatsapp_found': df['whatsapp_found'].sum(),
                'total_validated_emails': df['validated_emails_count'].sum(),
                'average_processing_time': df['processing_time'].mean(),
                'success_rate': (len(df[df['scraping_status'] == 'success']) / len(df) * 100) if len(df) > 0 else 0
            }
            
            # Generate report content
            report_content = self._generate_report_content(stats, df)
            
            # Save report
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            # Also save as JSON
            json_file = output_file.replace('.txt', '.json')
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, default=str)
            
            self.logger.info(f"📈 Report generated: {os.path.basename(output_file)}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error generating summary report: {str(e)}")
            raise
    
    def _generate_report_content(self, stats: Dict[str, Any], df: pd.DataFrame) -> str:
        """
        Generate formatted report content.
        
        Args:
            stats: Statistics dictionary
            df: Processed DataFrame
            
        Returns:
            Formatted report string
        """
        report = f"""
EMAIL SCRAPER & VALIDATOR - PROCESSING REPORT
============================================

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

OVERVIEW
--------
Total Records Processed: {stats['total_records']:,}
Successful Scrapes: {stats['successful_scrapes']:,}
Failed Scrapes: {stats['failed_scrapes']:,}
Success Rate: {stats['success_rate']:.1f}%
Average Processing Time: {stats['average_processing_time']:.2f} seconds

CONTACT EXTRACTION RESULTS
--------------------------
Records with Emails: {stats['records_with_emails']:,} ({stats['records_with_emails']/stats['total_records']*100:.1f}%)
Records with Phone Numbers: {stats['records_with_phones']:,} ({stats['records_with_phones']/stats['total_records']*100:.1f}%)
Records with WhatsApp: {stats['records_with_whatsapp']:,} ({stats['records_with_whatsapp']/stats['total_records']*100:.1f}%)
Records with Validated Emails: {stats['records_with_validated_emails']:,} ({stats['records_with_validated_emails']/stats['total_records']*100:.1f}%)

TOTAL CONTACTS FOUND
-------------------
Total Emails: {stats['total_emails_found']:,}
Total Phone Numbers: {stats['total_phones_found']:,}
Total WhatsApp Contacts: {stats['total_whatsapp_found']:,}
Total Validated Emails: {stats['total_validated_emails']:,}

PERFORMANCE METRICS
------------------
Average Emails per Successful Record: {stats['total_emails_found']/stats['successful_scrapes']:.1f}
Average Phones per Successful Record: {stats['total_phones_found']/stats['successful_scrapes']:.1f}
Email Validation Rate: {stats['total_validated_emails']/stats['total_emails_found']*100:.1f}%

TOP ERROR TYPES
--------------
"""
        
        # Add error analysis if available
        if 'scraping_error' in df.columns:
            error_counts = df[df['scraping_error'] != '']['scraping_error'].value_counts().head(5)
            for error, count in error_counts.items():
                report += f"{error}: {count} occurrences\n"
        
        return report


if __name__ == "__main__":
    # Example usage
    processor = PostProcessor()
    
    # Create wide-form output
    stats = processor.create_wide_form_output(
        input_file="processed_results.csv",
        output_file="wide_form_results.csv",
        max_contacts_per_type=5
    )
    
    print(f"Wide-form conversion completed: {stats['output_columns']} columns created")
    
    # Generate summary report
    report_stats = processor.generate_summary_report(
        input_file="processed_results.csv",
        output_file="processing_report.txt"
    )
    
    print(f"Summary report generated with {report_stats['success_rate']:.1f}% success rate")