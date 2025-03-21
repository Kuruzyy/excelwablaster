import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import sys, os, re, shutil, tempfile, platform, urllib.parse, logging, random, time, threading, queue
import pandas as pd
import psutil
from threading import Lock
from openpyxl import load_workbook
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, NoSuchWindowException, WebDriverException

# Webdriver managers
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.chrome.service import Service as ChromiumService
from webdriver_manager.chrome import ChromeDriverManager as BraveChromeDriverManager  # for Brave

# ---------------------------
# CONFIGURATION
# ---------------------------
CONFIG = {
    "SHEETS": {
        "LIST": "LIST",
        "MSGS": "MSGS",
        "DOCS": "DOCS",
        "MEDIA": "MEDIA",
        "SETTINGS": "SETTINGS",
    },
    "COLUMNS": {
        "LIST": {
            "sender": "Sender",
            "phone": "Phone Number",
            "name": "Name",
            "course": "Course of Interest",
            "msg_code": "Message Code",
            "doc_code": "Document Code",
            "media_code": "Media Code",
            "status": "Status",
        },
        "MSGS": {
            "msg_code": "Message Code",
            "message": "Message Encoded",
        },
        "DOCS": {
            "code": "Document Code",
            "files": ["BROCHURE_1", "BROCHURE_2", "BROCHURE_3", "BROCHURE_4"],
        },
        "MEDIA": {
            "code": "Media Code",
            "files": ["MEDIA_1", "MEDIA_2", "MEDIA_3", "MEDIA_4"],
        },
        "Settings": {
            "wd_chrome_ver": "WD_CHROME_VER",
            "wd_edge_ver": "WD_EDGE_VER",
            "wd_brave_ver": "WD_BRAVE_VER",
            "xpath_text": "XPATH_TEXT",
            "xpath_send": "XPATH_SEND",
            "xpath_attach": "XPATH_ATTACH",
            "xpath_asend": "XPATH_ASEND",
            "xpath_docs": "XPATH_DOCS",
            "xpath_media": "XPATH_MEDIA",
            "invalid_message": "INVALID_MSG",
            "min_timer": "MIN_TIMER",
            "max_timer": "MAX_TIMER",
        },
    },
    "STATUS_VALUES": {
        "INVALID": 0,
        "SENT": 1,
        "RETRY": 2,
    },
}

# ---------------------------
# Logging and Utility Functions
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("whatsapp_blaster.log"), logging.StreamHandler()]
)

def get_persistent_temp_path(instance_id=None):
    if instance_id is not None:
        path = os.path.join(tempfile.gettempdir(), f"whatsapp_blaster_data_{instance_id}")
    else:
        path = os.path.join(tempfile.gettempdir(), "whatsapp_blaster_data")
    if not os.path.exists(path):
        os.makedirs(path)
    return path

# Create separate user data paths for each browser instance
user_data_path_1 = get_persistent_temp_path("1")
user_data_path_2 = get_persistent_temp_path("2")
stop_event = threading.Event()

# Log queues for system and browser logs
system_log_queue = queue.Queue()
browser1_log_queue = queue.Queue()
browser2_log_queue = queue.Queue()

def async_log_worker(log_text, log_q):
    """Background worker that updates the given text widget from the provided log queue."""
    while True:
        try:
            message = log_q.get(timeout=1)
            log_text.after(0, lambda: log_text.insert(tk.END, message + "\n"))
            log_text.after(0, log_text.see, tk.END)
        except queue.Empty:
            continue

def log_system(message):
    system_log_queue.put(message)

def log_browser1(message):
    browser1_log_queue.put(message)

def log_browser2(message):
    browser2_log_queue.put(message)

def log_by_instance(instance_id, message):
    if instance_id == 1:
        log_browser1(message)
    elif instance_id == 2:
        log_browser2(message)
    else:
        log_system(message)

def log_message(log_text, message):
    log_text.insert(tk.END, f"{message}\n")
    log_text.see(tk.END)

def normalize_value(value):
    if isinstance(value, float) and value.is_integer():
        return str(int(value)).strip()
    return str(value).strip()

excel_lock = Lock()

def is_file_locked(filepath):
    try:
        with open(filepath, 'a'):
            return False
    except IOError:
        return True

def update_excel_status(excel_file, phone_number, new_status):
    attempts = 3
    while attempts > 0:
        if is_file_locked(excel_file):
            logging.warning(f"Excel file is locked. Retrying in 2 seconds for phone {phone_number}...")
            time.sleep(2)
            attempts -= 1
        else:
            break
    with excel_lock:
        try:
            wb = load_workbook(excel_file)
            sheet = wb[CONFIG["SHEETS"]["LIST"]]
            headers = [cell.value for cell in next(sheet.iter_rows(min_row=1, max_row=1))]
            phone_idx = headers.index(CONFIG["COLUMNS"]["LIST"]["phone"])
            status_idx = headers.index(CONFIG["COLUMNS"]["LIST"]["status"])
            updated = False
            for row in sheet.iter_rows(min_row=2):
                if normalize_value(row[phone_idx].value) == phone_number:
                    row[status_idx].value = new_status
                    updated = True
                    break
            if updated:
                wb.save(excel_file)
                logging.info(f"Updated status for {phone_number} to {new_status}")
            else:
                logging.warning(f"Phone number {phone_number} not found in Excel for updating.")
        except Exception as e:
            logging.error(f"Failed to update status for {phone_number}: {e}")

# ---------------------------
# Personalized Message Function
# ---------------------------
def parse_spintax(text):
    """ Process Spintax: Randomly selects one option from {word1 | word2 | word3} """
    pattern = re.compile(r"\[([^{}\[\]]*)\]")  # Matches text inside [ ]
    
    while re.search(pattern, text):  # Keep replacing spintax until none are left
        text = re.sub(pattern, lambda m: random.choice(m.group(1).split("|")).strip(), text)
    
    return text

def personalize_message(encoded_template, contact):
    # Step 1: Decode the URL-encoded template
    decoded_template = urllib.parse.unquote_plus(encoded_template)
    
    # Step 2: Replace placeholders with contact details
    personalized_message = decoded_template.format(
        name=contact.get("Name", ""),
        sender=contact.get("Sender", ""),
        course=contact.get("Course of Interest", "")
    )

    # Step 3: Process Spintax first
    spintax_processed = parse_spintax(personalized_message)

    # Step 4: Re-encode the message for WhatsApp URL
    return urllib.parse.quote_plus(spintax_processed)

# ---------------------------
# Excel Data Loader
# ---------------------------
class ExcelDataLoader:
    def __init__(self, excel_file):
        self.excel_file = excel_file
        self.sheets = self.load_all_sheets()

    def load_all_sheets(self):
        try:
            wb = load_workbook(self.excel_file)
            return {sheet: pd.read_excel(self.excel_file, sheet_name=sheet) for sheet in wb.sheetnames}
        except Exception as e:
            logging.error(f"Error loading Excel file: {e}")
            return {}

    def get_contacts(self):
        df = self.sheets.get(CONFIG["SHEETS"]["LIST"])
        if df is not None:
            df[CONFIG["COLUMNS"]["LIST"]["phone"]] = df[CONFIG["COLUMNS"]["LIST"]["phone"]].apply(normalize_value)
            df[CONFIG["COLUMNS"]["LIST"]["msg_code"]] = df[CONFIG["COLUMNS"]["LIST"]["msg_code"]].apply(normalize_value)
            df[CONFIG["COLUMNS"]["LIST"]["doc_code"]] = df[CONFIG["COLUMNS"]["LIST"]["doc_code"]].apply(normalize_value)
            df[CONFIG["COLUMNS"]["LIST"]["media_code"]] = df[CONFIG["COLUMNS"]["LIST"]["media_code"]].apply(normalize_value)
        return df

    def get_messages(self):
        df = self.sheets.get(CONFIG["SHEETS"]["MSGS"])
        if df is not None:
            df[CONFIG["COLUMNS"]["MSGS"]["msg_code"]] = df[CONFIG["COLUMNS"]["MSGS"]["msg_code"]].apply(normalize_value)
        return df

    def get_mapping(self, sheet_key, col_config):
        mapping = {}
        df = self.sheets.get(CONFIG["SHEETS"][sheet_key])
        if df is not None:
            df[col_config["code"]] = df[col_config["code"]].apply(normalize_value)
            for _, row in df.iterrows():
                key = row[col_config["code"]]
                mapping[key] = [row[col] for col in col_config["files"]]
        return mapping

    def get_docs(self):
        return self.get_mapping("DOCS", CONFIG["COLUMNS"]["DOCS"])

    def get_media(self):
        return self.get_mapping("MEDIA", CONFIG["COLUMNS"]["MEDIA"])

    def get_settings(self):
        """
        Load settings from the 'SETTINGS' sheet.
        Assumes the sheet has columns 'Setting Name' and 'Value'.
        """
        if CONFIG["SHEETS"]["SETTINGS"] in self.sheets:
            df = self.sheets[CONFIG["SHEETS"]["SETTINGS"]]
            return dict(zip(df["Setting Name"], df["Value"]))
        return {}

# ---------------------------
# Browser Manager
# ---------------------------
class BrowserManager:
    def __init__(self, instance_id=None):
        self.driver = None
        self.instance_id = instance_id
        self.user_data_path = get_persistent_temp_path(instance_id)

    def setup_browser(self, headless=False):
        if self.driver is not None:
            try:
                _ = self.driver.current_url
                return self.driver
            except Exception:
                self.driver = None
        options = Options()
        options.add_argument(f"--user-data-dir={self.user_data_path}")
        options.add_argument(f"--remote-debugging-port={9222 if self.instance_id is None else 9223 + int(self.instance_id)}")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-default-apps")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--no-first-run")
        options.add_argument("--no-service-autorun")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
                             "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        if headless:
            options.add_argument("--headless")
        browser_path, browser_type = self.locate_browser()
        if not browser_path:
            raise RuntimeError("No supported browser found on the system.")
        options.binary_location = browser_path
        # Use settings loaded from Excel for driver version
        if browser_type == "chrome":
            service = ChromeService(ChromeDriverManager(driver_version=settings_dict[CONFIG["COLUMNS"]["Settings"]["wd_chrome_ver"]]).install())
        elif browser_type == "edge":
            service = EdgeService(EdgeChromiumDriverManager(driver_version=settings_dict[CONFIG["COLUMNS"]["Settings"]["wd_edge_ver"]]).install())
        elif browser_type == "brave":
            service = ChromeService(BraveChromeDriverManager(chrome_type=ChromeType.BRAVE, driver_version=settings_dict[CONFIG["COLUMNS"]["Settings"]["wd_brave_ver"]]).install())
        else:
            service = ChromiumService(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install())
        self.driver = webdriver.Chrome(service=service, options=options)
        log_system(f"Browser instance {self.instance_id} initialized")
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
            raise RuntimeError(f"Unsupported OS: {system}")
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

    def quit(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

# ---------------------------
# WhatsApp Messaging Functions
# ---------------------------
def send_text_message(driver, phone_number, encoded_message, contact, instance_id):
    # Apply personalization & Spintax
    final_encoded_message = personalize_message(encoded_message, contact)
    
    # WhatsApp URL with the final personalized and randomized message
    url = f"https://web.whatsapp.com/send?phone={phone_number}&text={final_encoded_message}"
    
    driver.get(url)
    try:
        WebDriverWait(driver, 10).until(
            lambda d: CONFIG["INVALID_MSG"] in d.page_source or 
                      d.find_element(By.XPATH, settings_dict[CONFIG["COLUMNS"]["Settings"]["xpath_text"]])
        )
    except TimeoutException:
        log_system(f"[Instance {instance_id}] Timeout waiting for elements for {phone_number}.")
        return False
    
    if CONFIG["INVALID_MSG"] in driver.page_source:
        log_system(f"[Instance {instance_id}] Invalid phone number {phone_number}.")
        return "INVALID"

    time.sleep(random.uniform(float(settings_dict[CONFIG["COLUMNS"]["Settings"]["min_timer"]]),
                                float(settings_dict[CONFIG["COLUMNS"]["Settings"]["max_timer"]])))

    try:
        driver.find_element(By.XPATH, settings_dict[CONFIG["COLUMNS"]["Settings"]["xpath_text"]]).click()
        time.sleep(1)
        log_system(f"[Instance {instance_id}] Text message sent to {phone_number}.")
        return True
    except NoSuchElementException:
        log_system(f"[Instance {instance_id}] Send button not found for {phone_number}.")
        return False

def attach_files(driver, phone_number, file_paths, xpath, instance_id):
    whatsapp_url = f"https://web.whatsapp.com/send?phone={phone_number}"
    driver.get(whatsapp_url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, settings_dict[CONFIG["COLUMNS"]["Settings"]["xpath_attach"]]))
        )
        attach_btn = driver.find_element(By.XPATH, settings_dict[CONFIG["COLUMNS"]["Settings"]["xpath_attach"]])
        time.sleep(1)
        attach_btn.click()
        try:
            option = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
            option.click()
        except TimeoutException:
            log_system(f"[Instance {instance_id}] Could not find document/media option for {phone_number}.")
            return False
        try:
            file_input = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//input[@accept='*']"))
            )
            valid_paths = [str(p).strip() for p in file_paths if pd.notna(p)]
            if valid_paths:
                file_input.send_keys("\n".join(valid_paths))
                time.sleep(2)
                send_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, settings_dict[CONFIG["COLUMNS"]["Settings"]["xpath_asend"]]))
                )
                send_btn.click()
                log_system(f"[Instance {instance_id}] Files sent to {phone_number}.")
                return True
            else:
                log_system(f"[Instance {instance_id}] No valid files for {phone_number} using {xpath}.")
                return False
        except TimeoutException:
            log_system(f"[Instance {instance_id}] File input field not found for {phone_number}.")
            return False
    except Exception as e:
        log_system(f"[Instance {instance_id}] Error attaching files for {phone_number}: {e}")
        return False

def process_contact(driver, contact, messages_df, docs_mapping, media_mapping, excel_file, instance_id):
    phone = contact.get(CONFIG["COLUMNS"]["LIST"]["phone"])
    msg_code = contact.get(CONFIG["COLUMNS"]["LIST"]["msg_code"])
    doc_code = contact.get(CONFIG["COLUMNS"]["LIST"]["doc_code"])
    media_code = contact.get(CONFIG["COLUMNS"]["LIST"]["media_code"])
    sent_successfully = False

    if msg_code:
        message_row = messages_df[messages_df[CONFIG["COLUMNS"]["MSGS"]["msg_code"]] == msg_code]
        if not message_row.empty:
            encoded = message_row.iloc[0][CONFIG["COLUMNS"]["MSGS"]["message"]]
            
            # Pass the whole contact dictionary to personalize and apply Spintax
            result = send_text_message(driver, phone, encoded, contact, instance_id)
            
            if result == "INVALID":
                update_excel_status(excel_file, phone, CONFIG["STATUS_VALUES"]["INVALID"])
                return
            elif result:
                sent_successfully = True

    if doc_code and doc_code != "0" and doc_code in docs_mapping:
        result = attach_files(driver, phone, docs_mapping[doc_code], settings_dict[CONFIG["COLUMNS"]["Settings"]["xpath_docs"]], instance_id)
        sent_successfully = sent_successfully or result
    else:
        log_by_instance(instance_id, f"Skipping document attachment for {phone} (Doc Code: {doc_code}).")

    if media_code and media_code != "0" and media_code in media_mapping:
        result = attach_files(driver, phone, media_mapping[media_code], settings_dict[CONFIG["COLUMNS"]["Settings"]["xpath_media"]], instance_id)
        sent_successfully = sent_successfully or result
    else:
        log_by_instance(instance_id, f"Skipping media attachment for {phone} (Media Code: {media_code}).")

    if sent_successfully:
        update_excel_status(excel_file, phone, CONFIG["STATUS_VALUES"]["SENT"])
    else:
        update_excel_status(excel_file, phone, CONFIG["STATUS_VALUES"]["RETRY"])

    time.sleep(random.uniform(float(settings_dict[CONFIG["COLUMNS"]["Settings"]["min_timer"]]),
                              float(settings_dict[CONFIG["COLUMNS"]["Settings"]["max_timer"]])))

# ---------------------------
# Dual-Browser WhatsApp Blaster Implementation
# ---------------------------
class DualBrowserWhatsAppBlaster:
    def __init__(self):
        self.browser_manager_1 = BrowserManager(instance_id=1)
        self.browser_manager_2 = BrowserManager(instance_id=2)
        self.log_updater_active = False
        self.thread_log_updater = None

    def start_log_updater(self, log_text):
        self.log_updater_active = True
        def update_logs():
            while self.log_updater_active:
                try:
                    message = system_log_queue.get(timeout=0.1)
                    log_text.after(0, lambda: log_text.insert(tk.END, message + "\n"))
                    log_text.after(0, log_text.see, tk.END)
                except queue.Empty:
                    pass
                except Exception as e:
                    logging.error(f"Error in log updater: {e}")
        self.thread_log_updater = threading.Thread(target=update_logs)
        self.thread_log_updater.daemon = True
        self.thread_log_updater.start()

    def stop_log_updater(self):
        self.log_updater_active = False
        if self.thread_log_updater:
            self.thread_log_updater.join(timeout=1.0)
            self.thread_log_updater = None

    def process_contacts_thread(self, driver, contacts_df, messages_df, docs_mapping, media_mapping, excel_file, instance_id):
        try:
            driver.get("https://web.whatsapp.com")
            log_by_instance(instance_id, f"[Instance {instance_id}] Waiting for WhatsApp Web to load...")
            WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.XPATH, "//div[@id='pane-side']")))
            log_by_instance(instance_id, f"[Instance {instance_id}] WhatsApp Web loaded successfully.")
            for _, contact in contacts_df.iterrows():
                if stop_event.is_set():
                    log_by_instance(instance_id, f"[Instance {instance_id}] Stopping processing due to user request.")
                    break
                if contact.get(CONFIG["COLUMNS"]["LIST"]["status"]) in [CONFIG["STATUS_VALUES"]["INVALID"], CONFIG["STATUS_VALUES"]["SENT"]]:
                    continue
                try:
                    process_contact(driver, contact, messages_df, docs_mapping, media_mapping, excel_file, instance_id)
                except Exception as e:
                    log_by_instance(instance_id, f"[Instance {instance_id}] Error processing {contact.get(CONFIG['COLUMNS']['LIST']['phone'])}: {e}")
                    update_excel_status(excel_file, contact.get(CONFIG["COLUMNS"]["LIST"]["phone"]), CONFIG["STATUS_VALUES"]["RETRY"])
            log_by_instance(instance_id, f"[Instance {instance_id}] Initial processing complete.")
        except Exception as e:
            log_by_instance(instance_id, f"[Instance {instance_id}] Thread error: {e}")

    def retry_failed_contacts_thread(self, driver, retry_df, messages_df, docs_mapping, media_mapping, excel_file, instance_id):
        try:
            if retry_df.empty:
                log_by_instance(instance_id, f"[Instance {instance_id}] No contacts to retry.")
                return
            driver.get("https://web.whatsapp.com")
            WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.XPATH, "//div[@id='pane-side']")))
            for _, contact in retry_df.iterrows():
                if stop_event.is_set():
                    log_by_instance(instance_id, f"[Instance {instance_id}] Stopping retry process due to user request.")
                    break
                try:
                    process_contact(driver, contact, messages_df, docs_mapping, media_mapping, excel_file, instance_id)
                except Exception as e:
                    log_by_instance(instance_id, f"[Instance {instance_id}] Retry error for {contact.get(CONFIG['COLUMNS']['LIST']['phone'])}: {e}")
                    update_excel_status(excel_file, contact.get(CONFIG["COLUMNS"]["LIST"]["phone"]), CONFIG["STATUS_VALUES"]["INVALID"])
            log_by_instance(instance_id, f"[Instance {instance_id}] Retry process completed for instance {instance_id}.")
        except Exception as e:
            log_by_instance(instance_id, f"[Instance {instance_id}] Thread error in retry: {e}")

    def send_messages(self, log_text, excel_file, headless):
        stop_event.clear()
        self.start_log_updater(log_text)
        log_system("Starting dual browser WhatsApp blaster...")
        loader = ExcelDataLoader(excel_file)
        global settings_dict
        settings_dict = loader.get_settings()
        if settings_dict:
            CONFIG["WEBDRIVER_VER"] = {
                "CHROME": settings_dict[CONFIG["COLUMNS"]["Settings"]["wd_chrome_ver"]],
                "EDGE": settings_dict[CONFIG["COLUMNS"]["Settings"]["wd_edge_ver"]],
                "BRAVE": settings_dict[CONFIG["COLUMNS"]["Settings"]["wd_brave_ver"]]
            }
            CONFIG["INVALID_MSG"] = settings_dict[CONFIG["COLUMNS"]["Settings"]["invalid_message"]]
            CONFIG["TIMER"] = {
                "MIN": float(settings_dict[CONFIG["COLUMNS"]["Settings"]["min_timer"]]),
                "MAX": float(settings_dict[CONFIG["COLUMNS"]["Settings"]["max_timer"]])
            }
        contacts = loader.get_contacts()
        messages = loader.get_messages()
        docs = loader.get_docs()
        media = loader.get_media()
        even_contacts = contacts.iloc[::2].copy()
        odd_contacts = contacts.iloc[1::2].copy()
        log_system(f"Total contacts: {len(contacts)}, Even: {len(even_contacts)}, Odd: {len(odd_contacts)}")
        try:
            driver1 = self.browser_manager_1.setup_browser(headless)
            driver2 = self.browser_manager_2.setup_browser(headless)
            thread1 = threading.Thread(
                target=self.process_contacts_thread,
                args=(driver1, even_contacts, messages, docs, media, excel_file, 1)
            )
            thread2 = threading.Thread(
                target=self.process_contacts_thread,
                args=(driver2, odd_contacts, messages, docs, media, excel_file, 2)
            )
            thread1.start()
            thread2.start()
            thread1.join()
            thread2.join()
            log_system("Initial processing complete in both browsers.")
            loader = ExcelDataLoader(excel_file)
            contacts = loader.get_contacts()
            retry_df = contacts[contacts[CONFIG["COLUMNS"]["LIST"]["status"]] == CONFIG["STATUS_VALUES"]["RETRY"]]
            log_system(f"{len(retry_df)} contacts marked for retry.")
            if not retry_df.empty:
                even_retry = retry_df.iloc[::2].copy()
                odd_retry = retry_df.iloc[1::2].copy()
                retry_thread1 = threading.Thread(
                    target=self.retry_failed_contacts_thread,
                    args=(driver1, even_retry, messages, docs, media, excel_file, 1)
                )
                retry_thread2 = threading.Thread(
                    target=self.retry_failed_contacts_thread,
                    args=(driver2, odd_retry, messages, docs, media, excel_file, 2)
                )
                retry_thread1.start()
                retry_thread2.start()
                retry_thread1.join()
                retry_thread2.join()
            log_system("All processing completed. Closing browsers.")
        except Exception as e:
            log_system(f"Error in dual browser operation: {e}")
        finally:
            self.browser_manager_1.quit()
            self.browser_manager_2.quit()
            self.stop_log_updater()

# ---------------------------
# GUI and Main Execution
# ---------------------------
def create_gui():
    root = tk.Tk()
    root.title("HWUM WhatsApp Blaster")
    root.geometry("480x500")
    root.configure(bg="#333333")
    root.resizable(False, False)
    tk.Label(root, text="HWUM WhatsApp Blaster", bg="#333333", fg="white", font=("Arial", 20)).pack(pady=5)
    button_frame = tk.Frame(root, bg="#333333")
    button_frame.pack(pady=10)
    excel_file = tk.StringVar()
    headless_mode = tk.BooleanVar(value=False)
    blaster = DualBrowserWhatsAppBlaster()
    def download_template():
        try:
            base_path = sys._MEIPASS if getattr(sys, 'frozen', False) else os.path.abspath(".")
            template_path = os.path.join(base_path, "Template.xlsx")
            destination = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel Files", "*.xlsx")], initialfile="Template.xlsx", title="Save Template As")
            if destination:
                shutil.copyfile(template_path, destination)
                messagebox.showinfo("Download Template", f"Template saved to {destination}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to download template: {e}")
    def import_excel():
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsm *.xlsx")])
        if path:
            excel_file.set(path)
            log_system(f"Excel file loaded: {path}")
            loader = ExcelDataLoader(path)
            global settings_dict
            settings_dict = loader.get_settings()
            if settings_dict:
                log_system("Settings successfully loaded from Excel.")
            else:
                log_system("Warning: No settings found in the Excel file.")
    def first_time_setup_wrapper():
        if not excel_file.get():
            messagebox.showerror("Error", "Please load an Excel file first.")
            return
        if not settings_dict:
            messagebox.showerror("Error", "Settings not loaded. Please check your Excel file.")
            return
        def setup_browsers():
            try:
                blaster.browser_manager_1.setup_browser(headless_mode.get())
                blaster.browser_manager_2.setup_browser(headless_mode.get())
                log_system("Both browser instances launched. Scan QR codes to login.")
            except Exception as e:
                log_system(f"Error initializing browsers: {e}")
        threading.Thread(target=setup_browsers).start()
    def send_messages_wrapper():
        if not excel_file.get():
            messagebox.showerror("Error", "Please load an Excel file first.")
            return
        threading.Thread(target=blaster.send_messages, args=(system_log_text, excel_file.get(), headless_mode.get())).start()
    def stop_process():
        stop_event.set()
        log_system("Stopping process...")
        blaster.browser_manager_1.quit()
        blaster.browser_manager_2.quit()
        for proc in psutil.process_iter(attrs=["pid", "name"]):
            if "chromedriver" in proc.info["name"].lower():
                try:
                    proc.terminate()
                except psutil.NoSuchProcess:
                    pass
        log_system("All processes stopped.")
    def toggle_headless():
        headless_mode.set(not headless_mode.get())
        headless_button.config(text="Headless" if headless_mode.get() else "Not Headless")
    def open_encoder():
        enc_win = tk.Toplevel()
        enc_win.title("Message Encoder")
        enc_win.resizable(False, False)
        ttk.Label(enc_win, text="Input Message:").pack(padx=10, pady=(10,0), anchor="w")
        input_text = tk.Text(enc_win, height=10, width=50)
        input_text.pack(padx=10, pady=(0,10))
        ttk.Label(enc_win, text="Encoded Message:").pack(padx=10, pady=(10,0), anchor="w")
        output_text = tk.Text(enc_win, height=10, width=50)
        output_text.pack(padx=10, pady=(0,10))
        def encode_message():
            msg = input_text.get("1.0", tk.END).strip()
            encoded = urllib.parse.quote_plus(msg)
            output_text.delete("1.0", tk.END)
            output_text.insert(tk.END, encoded)
        ttk.Button(enc_win, text="Encode Message", command=encode_message).pack(pady=(0,10))
    ttk.Button(root, text="Encoder", command=open_encoder, width=65).pack(pady=2)
    ttk.Button(button_frame, text="Download Template", width=20, command=download_template).grid(row=0, column=0, padx=5, pady=5)
    ttk.Button(button_frame, text="Import Excel", width=20, command=import_excel).grid(row=0, column=1, padx=5, pady=5)
    ttk.Button(button_frame, text="Launch WA Web", width=20, command=first_time_setup_wrapper).grid(row=0, column=2, padx=5, pady=5)
    ttk.Button(button_frame, text="RUN", width=20, command=send_messages_wrapper).grid(row=1, column=0, padx=5, pady=5)
    ttk.Button(button_frame, text="STOP", width=20, command=stop_process).grid(row=1, column=1, padx=5, pady=5)
    headless_button = ttk.Button(button_frame, text="Not Headless", command=toggle_headless, width=20)
    headless_button.grid(row=1, column=2, padx=5, pady=5)
    tk.Label(root, text="Logs", bg="#333333", fg="white", font=("Arial", 12)).pack(anchor="w", padx=10)
    system_log_text = tk.Text(root, width=55, height=5, bg="lightyellow")
    system_log_text.pack(pady=5)
    browser1_log_text = tk.Text(root, width=55, height=5, bg="lightblue")
    browser1_log_text.pack(pady=5)
    browser2_log_text = tk.Text(root, width=55, height=5, bg="lightgreen")
    browser2_log_text.pack(pady=5)
    threading.Thread(target=async_log_worker, args=(browser1_log_text, browser1_log_queue), daemon=True).start()
    threading.Thread(target=async_log_worker, args=(browser2_log_text, browser2_log_queue), daemon=True).start()
    tk.Label(root, text="qt3000@hw.ac.uk", bg="#333333", fg="white", font=("Arial", 10)).pack(side=tk.BOTTOM, pady=1)
    def cleanup():
        blaster.browser_manager_1.quit()
        blaster.browser_manager_2.quit()
        root.destroy()
    root.protocol("WM_DELETE_WINDOW", cleanup)
    root.mainloop()

if __name__ == "__main__":
    create_gui()
