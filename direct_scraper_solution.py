#!/usr/bin/env python3
"""
Direct Scraper Solution - Bypass CSV pipeline issues
Use direct WebScraper calls that are proven to work
"""

import sys
import csv
import time
from web_scraper import WebScraper
from contact_extractor import ContactExtractor
from email_validation import EmailValidator

def direct_scrape_url(url: str, output_file: str = None):
    """
    Direct scraping menggunakan WebScraper yang proven berhasil
    """
    
    print(f"🎯 Direct scraping: {url}")
    print("=" * 80)
    
    # Initialize with proven config
    scraper = WebScraper(
        timeout=60,
        cf_wait_timeout=120,
        solve_cloudflare=True,
        network_idle=False,  # Based on successful tests
        block_images=False,
        disable_resources=False
    )
    
    extractor = ContactExtractor()
    validator = EmailValidator()
    
    # Scrape URL
    print("⏳ Starting scrape...")
    start_time = time.time()
    
    result = scraper.scrape_url(url)
    
    elapsed = time.time() - start_time
    
    print(f"⏱️  Scraping completed in {elapsed:.2f}s")
    print(f"📊 Status: {result.get('status')}")
    print(f"📄 HTML length: {len(result.get('html', ''))} bytes")
    print(f"❌ Error: {result.get('error', 'None')}")
    
    if result.get('status') == 200 and result.get('html'):
        print("\n✅ SCRAPING SUCCESS! Extracting contacts...")
        
        # Extract contacts
        contacts = extractor.extract_all_contacts(result['html'])
        
        emails = [c for c in contacts if c.get('field') == 'email']
        phones = [c for c in contacts if c.get('field') == 'phone']
        whatsapp = [c for c in contacts if c.get('field') == 'whatsapp']
        
        print(f"📧 Emails found: {len(emails)}")
        print(f"📱 Phones found: {len(phones)}")
        print(f"💬 WhatsApp found: {len(whatsapp)}")
        
        # Show results
        all_contacts = emails + phones + whatsapp
        if all_contacts:
            print(f"\n🎯 CONTACT EXTRACTION RESULTS:")
            for contact in all_contacts[:10]:  # Show first 10
                field = contact.get('field', 'unknown')
                value = contact.get('value_normalized') or contact.get('value_raw', 'N/A')
                print(f"   {field}: {value}")
        
        # Validate emails if found
        validated_emails = []
        if emails:
            print(f"\n📧 Validating {len(emails)} emails...")
            for email_contact in emails[:5]:  # Validate first 5
                email = email_contact.get('value_normalized') or email_contact.get('value_raw')
                if email:
                    validation = validator.validate_email(email)
                    validated_emails.append(f"{email}:{validation.get('status', 'unknown')}")
        
        # Create CSV output
        if output_file:
            print(f"\n📝 Writing results to {output_file}")
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Headers
                headers = [
                    'url', 'emails', 'phones', 'whatsapp', 'validated_emails',
                    'scraping_status', 'scraping_error', 'processing_time',
                    'pages_scraped', 'emails_found', 'phones_found', 
                    'whatsapp_found', 'validated_emails_count'
                ]
                writer.writerow(headers)
                
                # Data
                emails_str = ';'.join([c.get('value_normalized') or c.get('value_raw', '') for c in emails])
                phones_str = ';'.join([c.get('value_normalized') or c.get('value_raw', '') for c in phones])
                whatsapp_str = ';'.join([c.get('value_normalized') or c.get('value_raw', '') for c in whatsapp])
                validated_str = ';'.join(validated_emails)
                
                row = [
                    url,
                    emails_str,
                    phones_str, 
                    whatsapp_str,
                    validated_str,
                    'success',
                    '',
                    elapsed,
                    1,
                    len(emails),
                    len(phones),
                    len(whatsapp),
                    len(validated_emails)
                ]
                writer.writerow(row)
            
            print(f"✅ Results saved to {output_file}")
        
        print(f"\n🎉 MISSION ACCOMPLISHED!")
        print(f"   ✅ Cloudflare bypass: SUCCESS")
        print(f"   ✅ Content extraction: SUCCESS") 
        print(f"   ✅ Contact parsing: {len(all_contacts)} contacts found")
        print(f"   ✅ Processing time: {elapsed:.2f}s")
        
        return {
            'success': True,
            'status': result.get('status'),
            'contacts': all_contacts,
            'emails': emails,
            'phones': phones,
            'whatsapp': whatsapp,
            'processing_time': elapsed
        }
    else:
        print(f"\n❌ SCRAPING FAILED")
        error = result.get('error', 'Unknown error')
        print(f"   Error: {error}")
        
        if output_file:
            # Write failed result to CSV
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                headers = [
                    'url', 'emails', 'phones', 'whatsapp', 'validated_emails',
                    'scraping_status', 'scraping_error', 'processing_time',
                    'pages_scraped', 'emails_found', 'phones_found', 
                    'whatsapp_found', 'validated_emails_count'
                ]
                writer.writerow(headers)
                
                row = [url, '', '', '', '', 'failed', error, elapsed, 0, 0, 0, 0, 0]
                writer.writerow(row)
        
        return {
            'success': False,
            'error': error,
            'processing_time': elapsed
        }

if __name__ == "__main__":
    # Test dengan oxygenyogaandfitness.com
    url = "https://oxygenyogaandfitness.com"
    output_file = "results/oxy_direct_success.csv"
    
    result = direct_scrape_url(url, output_file)
    
    if result['success']:
        print(f"\n🏆 FINAL VICTORY! Successfully bypassed Cloudflare and extracted contacts!")
    else:
        print(f"\n💔 Still failed, but we isolated the issue to the CSV pipeline")