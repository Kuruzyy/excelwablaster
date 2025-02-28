import tkinter as tk
from tkinter import filedialog, scrolledtext, messagebox, ttk
import sys
import shutil
import os
import tempfile
import platform
import logging
import random
import time
import threading
import pandas as pd
from openpyxl import load_workbook  # For updating Excel

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchWindowException, WebDriverException, NoSuchElementException

from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager

# Default Timer Values
TIMER_MIN = 10
TIMER_MAX = 20

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("whatsapp_blaster.log"),
        logging.StreamHandler()
    ]
)

# Determine a persistent folder in the temp directory
def get_persistent_temp_path():
    temp_dir = tempfile.gettempdir()
    persistent_temp_path = os.path.join(temp_dir, "whatsapp_blaster_data")
    if not os.path.exists(persistent_temp_path):
        os.makedirs(persistent_temp_path)
    return persistent_temp_path

# User data path
user_data_path = get_persistent_temp_path()

# Global stop event
stop_event = threading.Event()

def update_excel_status(excel_file, phone_number, new_status):
    """
    Updates the status for a given phone number in the LIST sheet.
    new_status: 1 (sent), 0 (invalid), or 2 (retry)
    """
    try:
        wb = load_workbook(excel_file)
        sheet = wb["LIST"]
        # Assume the header row is row 1:
        for row in sheet.iter_rows(min_row=2):
            cell_val = row[0].value  # "Phone Number" is in column A
            if cell_val is not None and str(cell_val).strip() == phone_number:
                # Update the Status column (assumed to be column D, index 3)
                row[3].value = new_status
                break
        wb.save(excel_file)
    except Exception as e:
        logging.error(f"Failed to update status for {phone_number}: {e}")

def load_documents(excel_file):
    """
    Loads the DOCS sheet from the Excel file and returns a mapping of Message Code to list of document file paths.
    The DOCS sheet is expected to have columns: Message Code, INFO, BROCHURE_1, BROCHURE_2, BROCHURE_3, BROCHURE_4.
    """
    docs_mapping = {}
    try:
        docs_df = pd.read_excel(excel_file, sheet_name='DOCS')
        for index, row in docs_df.iterrows():
            code = row['Message Code']
            docs_mapping[code] = [
                row['BROCHURE_1'], row['BROCHURE_2'],
                row['BROCHURE_3'], row['BROCHURE_4']
            ]
    except Exception as e:
        print(f"Error loading DOCS sheet: {e}")
    return docs_mapping

class WhatsAppBlaster:
    def __init__(self):
        self.driver = None

    def setup_browser(self, headless=False):
        options = Options()
        options.add_argument(f"--user-data-dir={user_data_path}")
        options.add_argument("--remote-debugging-port=9222")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-default-apps")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--no-first-run")
        options.add_argument("--no-service-autorun")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        if headless:
            options.add_argument("--headless")
        browser_path, browser_type = self.locate_browser()
        if not browser_path:
            raise RuntimeError("No supported browser (Chrome, Brave, Edge) found on the system.")
        options.binary_location = browser_path
        if browser_type == "chrome":
            service = Service(ChromeDriverManager().install())
        elif browser_type == "edge":
            service = Service(EdgeChromiumDriverManager().install())
        else:
            service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        return self.driver

    def locate_browser(self):
        system = platform.system()
        if system == "Windows":
            brave_paths = [
                os.path.expandvars(r"%ProgramFiles%\BraveSoftware\Brave-Browser\Application\brave.exe"),
                os.path.expandvars(r"%ProgramFiles(x86)%\BraveSoftware\Brave-Browser\Application\brave.exe"),
                os.path.expandvars(r"%LocalAppData%\BraveSoftware\Brave-Browser\Application\brave.exe"),
            ]
            chrome_paths = [
                os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
                os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
                os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
            ]
            edge_paths = [
                os.path.expandvars(r"%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"),
                os.path.expandvars(r"%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe"),
            ]
        elif system == "Darwin":
            brave_paths = ["/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"]
            chrome_paths = ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"]
            edge_paths = ["/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"]
        elif system == "Linux":
            brave_paths = ["/usr/bin/brave-browser"]
            chrome_paths = ["/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"]
            edge_paths = ["/usr/bin/microsoft-edge"]
        else:
            raise RuntimeError(f"Unsupported operating system: {system}")
        for path in brave_paths:
            if os.path.exists(path):
                return path, "brave"
        for path in chrome_paths:
            if os.path.exists(path):
                return path, "chrome"
        for path in edge_paths:
            if os.path.exists(path):
                return path, "edge"
        return None, None

    def first_time_setup(self, log_text, headless=False):
        log_text.insert(tk.END, "Launching browser for first-time setup...\n")
        try:
            self.driver = self.setup_browser(headless=headless)
            self.driver.get("https://web.whatsapp.com")
            log_text.insert(tk.END, "Waiting for WhatsApp Web login...\n")
            WebDriverWait(self.driver, 300).until(
                EC.presence_of_element_located((By.XPATH, "//canvas[@aria-label='Scan me!']"))
            )
            log_text.insert(tk.END, "QR code loaded. Please scan with your phone.\n")
            WebDriverWait(self.driver, 300).until_not(
                EC.presence_of_element_located((By.XPATH, "//canvas[@aria-label='Scan me!']"))
            )
            log_text.insert(tk.END, "Logged into WhatsApp Web successfully.\n")
        except NoSuchWindowException:
            log_text.insert(tk.END, "Browser closed. Setup complete.\n")
        except WebDriverException as e:
            log_text.insert(tk.END, f"Unexpected error during setup: {e}\n")
        finally:
            try:
                self.driver.quit()
            except Exception:
                pass

    def send_messages(self, log_text, excel_file, timer_min, timer_max, headless=False):
        log_text.insert(tk.END, "Starting the WhatsApp message blaster...\n")
        try:
            self.driver = self.setup_browser(headless=headless)
            self.driver.get("https://web.whatsapp.com")
            log_text.insert(tk.END, "Waiting for WhatsApp Web to load...\n")
            WebDriverWait(self.driver, 120).until(
                EC.presence_of_element_located((By.XPATH, "//div[@id='pane-side']"))
            )
            log_text.insert(tk.END, "WhatsApp Web loaded successfully.\n")
            # Load Excel data from Template file
            contacts_df = pd.read_excel(excel_file, sheet_name='LIST')
            messages_df = pd.read_excel(excel_file, sheet_name='MSGS')
            docs_mapping = load_documents(excel_file)
            log_text.insert(tk.END, f"Found {len(contacts_df)} contacts, {len(messages_df)} message templates, and document mappings for {len(docs_mapping)} codes.\n")
            for index, contact in contacts_df.iterrows():
                if stop_event.is_set():
                    log_text.insert(tk.END, "Process stopped by user.\n")
                    break
                # Skip contacts already marked as sent (status 1)
                if contact.get('Status') == 1:
                    continue

                try:
                    # Ensure phone number is a string without a decimal point
                    num = contact['Phone Number']
                    if isinstance(num, float):
                        phone_number = str(int(num))
                    else:
                        phone_number = str(num).strip()
                    message_code = contact['Message Code']
                    # Look up the message by code in MSGS
                    message_row = messages_df[messages_df['Message Code'] == message_code]
                    if message_row.empty:
                        log_text.insert(tk.END, f"No message found for code {message_code}. Skipping {phone_number}.\n")
                        update_excel_status(excel_file, phone_number, 0)  # Mark invalid
                        continue
                    encoded_message = message_row.iloc[0]['Message Encoded']
                    # First, send the text message
                    whatsapp_url = f"https://web.whatsapp.com/send?phone={phone_number}&text={encoded_message}"
                    self.driver.get(whatsapp_url)
                    time.sleep(random.uniform(timer_min, timer_max))
                    try:
                        send_button = self.driver.find_element(By.XPATH, "//span[@data-icon='send']")
                        send_button.click()
                        log_text.insert(tk.END, f"Text message sent to {phone_number}.\n")
                    except NoSuchElementException:
                        log_text.insert(tk.END, f"Send button not found for {phone_number}. Marking as retry.\n")
                        update_excel_status(excel_file, phone_number, 2)  # Retry
                        continue

                    # If document attachments are needed, reenter chat and upload docs
                    if message_code in docs_mapping:
                        brochure_paths = docs_mapping[message_code]
                        # Reenter chat with only the phone number (no text)
                        whatsapp_url_no_text = f"https://web.whatsapp.com/send?phone={phone_number}"
                        self.driver.get(whatsapp_url_no_text)
                        time.sleep(random.uniform(timer_min, timer_max))
                        try:
                            attach_button = self.driver.find_element(By.XPATH, "//*[@id='main']/footer/div[1]/div/span/div/div[1]/div/button")
                            attach_button.click()
                            time.sleep(1)
                            doc_option = self.driver.find_element(By.XPATH, "//*[@id='app']/div/span[5]/div/ul/div/div/div[1]/li")
                            doc_option.click()
                            time.sleep(1)
                            file_input = self.driver.find_element(By.XPATH, "//input[@accept='*']")
                            file_input.send_keys("\n".join(brochure_paths))
                            time.sleep(3)  # Wait for documents to upload
                            log_text.insert(tk.END, f"Documents attached for {phone_number}.\n")
                            send_button = self.driver.find_element(By.XPATH, "//span[@data-icon='send']")
                            send_button.click()
                            log_text.insert(tk.END, f"Document message sent to {phone_number}.\n")
                        except Exception as e:
                            log_text.insert(tk.END, f"Error attaching documents for {phone_number}: {e}\n")
                            update_excel_status(excel_file, phone_number, 2)
                            continue
                    # If no document attachment is needed, or after document message is sent, mark as sent.
                    update_excel_status(excel_file, phone_number, 1)
                    time.sleep(random.uniform(timer_min, timer_max))
                except Exception as e:
                    log_text.insert(tk.END, f"Error processing {phone_number}: {e}\n")
                    update_excel_status(excel_file, phone_number, 2)  # Mark for retry
            log_text.insert(tk.END, "Initial processing complete. Closing browser.\n")
            self.driver.quit()
            # Now, attempt to retry messages marked with status 2
            self.retry_failed_messages(log_text, excel_file, timer_min, timer_max, headless)
        except Exception as e:
            log_text.insert(tk.END, f"Error: {e}\n")
        finally:
            stop_event.clear()

    def retry_failed_messages(self, log_text, excel_file, timer_min, timer_max, headless=False):
        log_text.insert(tk.END, "Starting retry process for messages marked as retry (status 2)...\n")
        try:
            # Reload contacts from Excel
            contacts_df = pd.read_excel(excel_file, sheet_name='LIST')
            # Filter contacts with status == 2
            retry_df = contacts_df[contacts_df['Status'] == 2]
            log_text.insert(tk.END, f"{len(retry_df)} contacts marked for retry.\n")
            if retry_df.empty:
                log_text.insert(tk.END, "No contacts to retry.\n")
                return
            # Reload document mappings for retries
            docs_mapping = load_documents(excel_file)
            log_text.insert(tk.END, f"Loaded document mappings for {len(docs_mapping)} codes during retry.\n")
            self.driver = self.setup_browser(headless=headless)
            self.driver.get("https://web.whatsapp.com")
            WebDriverWait(self.driver, 120).until(
                EC.presence_of_element_located((By.XPATH, "//div[@id='pane-side']"))
            )
            for index, contact in retry_df.iterrows():
                if stop_event.is_set():
                    log_text.insert(tk.END, "Retry process stopped by user.\n")
                    break
                try:
                    num = contact['Phone Number']
                    if isinstance(num, float):
                        phone_number = str(int(num))
                    else:
                        phone_number = str(num).strip()
                    message_code = contact['Message Code']
                    message_row = pd.read_excel(excel_file, sheet_name='MSGS')
                    message_row = message_row[message_row['Message Code'] == message_code]
                    if message_row.empty:
                        log_text.insert(tk.END, f"No message found for code {message_code}. Skipping retry for {phone_number}.\n")
                        update_excel_status(excel_file, phone_number, 0)
                        continue
                    encoded_message = message_row.iloc[0]['Message Encoded']
                    # Send text message for retry
                    whatsapp_url = f"https://web.whatsapp.com/send?phone={phone_number}&text={encoded_message}"
                    self.driver.get(whatsapp_url)
                    time.sleep(random.uniform(timer_min, timer_max))
                    try:
                        send_button = self.driver.find_element(By.XPATH, "//span[@data-icon='send']")
                        send_button.click()
                        log_text.insert(tk.END, f"Retry: Text message sent to {phone_number}.\n")
                    except NoSuchElementException:
                        log_text.insert(tk.END, f"Retry: Send button not found for {phone_number}. Marking as invalid.\n")
                        update_excel_status(excel_file, phone_number, 0)
                        continue
                    
                    if message_code in docs_mapping:
                        brochure_paths = docs_mapping[message_code]
                        # Reenter chat without text
                        whatsapp_url_no_text = f"https://web.whatsapp.com/send?phone={phone_number}"
                        self.driver.get(whatsapp_url_no_text)
                        time.sleep(random.uniform(timer_min, timer_max))
                        try:
                            attach_button = self.driver.find_element(By.XPATH, "//*[@id='main']/footer/div[1]/div/span/div/div[1]/div/button")
                            attach_button.click()
                            time.sleep(1)
                            doc_option = self.driver.find_element(By.XPATH, "//*[@id='app']/div/span[5]/div/ul/div/div/div[1]/li")
                            doc_option.click()
                            time.sleep(1)
                            file_input = self.driver.find_element(By.XPATH, "//input[@accept='*']")
                            file_input.send_keys("\n".join(brochure_paths))
                            time.sleep(3)
                            log_text.insert(tk.END, f"Retry: Documents attached for {phone_number}.\n")
                            send_button = self.driver.find_element(By.XPATH, "//span[@data-icon='send']")
                            send_button.click()
                            log_text.insert(tk.END, f"Retry: Document message sent to {phone_number}.\n")
                        except Exception as e:
                            log_text.insert(tk.END, f"Retry: Error attaching documents for {phone_number}: {e}\n")
                            update_excel_status(excel_file, phone_number, 0)
                            continue
                    update_excel_status(excel_file, phone_number, 1)
                    time.sleep(random.uniform(timer_min, timer_max))
                except Exception as e:
                    log_text.insert(tk.END, f"Retry: Error processing {phone_number}: {e}\n")
                    update_excel_status(excel_file, phone_number, 0)
            log_text.insert(tk.END, "Retry process completed. Closing browser.\n")
            self.driver.quit()
        except Exception as e:
            log_text.insert(tk.END, f"Retry process error: {e}\n")

# New function to download the template Excel file
def download_template():
    try:
        # When bundled using PyInstaller, the template will be in sys._MEIPASS
        if getattr(sys, 'frozen', False):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.abspath(".")
        template_path = os.path.join(base_path, "Template.xlsx")
        destination = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            initialfile="Template.xlsx",
            title="Save Template As"
        )
        if destination:
            shutil.copyfile(template_path, destination)
            messagebox.showinfo("Download Template", f"Template saved to {destination}")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to download template: {e}")

def create_gui():
    root = tk.Tk()
    root.title("WhatsApp Blaster (Excel Version)")
    root.geometry("430x360")
    root.configure(bg="#333333")
    root.resizable(width=False, height=False)
    tk.Label(root, text="WhatsApp Blaster", bg="#333333", fg="white", font=("Arial", 20)).pack(pady=5)
    # Frame for buttons
    button_frame = tk.Frame(root, bg="#333333")
    button_frame.pack(pady=10)
    excel_file = tk.StringVar()
    timer_min = tk.StringVar(value=str(TIMER_MIN))
    timer_max = tk.StringVar(value=str(TIMER_MAX))
    headless_mode = tk.BooleanVar(value=False)
    blaster = WhatsAppBlaster()

    def import_excel():
        file_path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsm *.xlsx")])
        if file_path:
            excel_file.set(file_path)
            log_text.insert(tk.END, f"Excel file loaded: {file_path}\n")
            try:
                contacts_df = pd.read_excel(file_path, sheet_name='LIST')
                messages_df = pd.read_excel(file_path, sheet_name='MSGS')
                docs_df = pd.read_excel(file_path, sheet_name='DOCS')
                log_text.insert(tk.END, f"Found {len(contacts_df)} contacts in LIST sheet.\n")
                log_text.insert(tk.END, f"Found {len(messages_df)} message templates in MSGS sheet.\n")
                log_text.insert(tk.END, f"Found {len(docs_df)} document sets in DOCS sheet.\n")
            except Exception as e:
                log_text.insert(tk.END, f"Error reading Excel file: {e}\n")
    
    def first_time_setup_wrapper():
        threading.Thread(target=blaster.first_time_setup, args=(log_text, headless_mode.get())).start()
    
    def send_messages_wrapper():
        if not excel_file.get():
            messagebox.showerror("Error", "Please load an Excel file first.")
            return
        try:
            timer_min_val = float(timer_min.get())
            timer_max_val = float(timer_max.get())
            if timer_min_val >= timer_max_val or timer_min_val < 0 or timer_max_val < 0:
                raise ValueError("Invalid timer values.")
        except ValueError:
            messagebox.showerror("Error", "Please enter valid numeric timer values.")
            return
        threading.Thread(target=blaster.send_messages, args=(log_text, excel_file.get(), timer_min_val, timer_max_val, headless_mode.get())).start()
    
    def stop_process():
        stop_event.set()
        log_text.insert(tk.END, "Stopping process...\n")
    
    # Arrange buttons
    ttk.Button(button_frame, text="Download Template", width=20, command=download_template).grid(row=0, column=0, padx=5, pady=5)
    ttk.Button(button_frame, text="Import Excel", width=20, command=import_excel).grid(row=0, column=1, padx=5, pady=5)
    ttk.Button(button_frame, text="Launch WA Web", width=20, command=first_time_setup_wrapper).grid(row=0, column=2, padx=5, pady=5)
    ttk.Button(button_frame, text="RUN", width=20, command=send_messages_wrapper).grid(row=1, column=0, padx=5, pady=5)
    ttk.Button(button_frame, text="STOP", width=20, command=stop_process).grid(row=1, column=1, padx=5, pady=5)
    
    
    def toggle_headless():
        if headless_mode.get():
            headless_mode.set(False)
            headless_button.config(text="Not Headless", style="TButton")
        else:
            headless_mode.set(True)
            headless_button.config(text="Headless", style="TButton")
    
    headless_button = ttk.Button(button_frame, text="Not Headless", command=toggle_headless, width=20)
    headless_button.grid(row=1, column=2, padx=5, pady=5)
    
    timer_frame = tk.Frame(root, bg="#333333")
    timer_frame.pack(pady=10)
    tk.Label(timer_frame, text="Min Timer (sec):", bg="#333333", fg="white").pack(side=tk.LEFT, padx=5)
    tk.Entry(timer_frame, textvariable=timer_min, width=5).pack(side=tk.LEFT, padx=5)
    tk.Label(timer_frame, text="Max Timer (sec):", bg="#333333", fg="white").pack(side=tk.LEFT, padx=5)
    tk.Entry(timer_frame, textvariable=timer_max, width=5).pack(side=tk.LEFT, padx=5)
    tk.Label(root, text="Logs", bg="#333333", fg="white", font=("Arial", 12)).pack(anchor="w", padx=10)
    log_text = scrolledtext.ScrolledText(root, width=70, height=7, font=("Arial", 10))
    log_text.pack(padx=10, pady=10)
    tk.Label(root, text="qt3000@hw.ac.uk", bg="#333333", fg="white", font=("Arial", 10)).pack(side=tk.BOTTOM, pady=1)
    root.mainloop()

if __name__ == "__main__":
    create_gui()