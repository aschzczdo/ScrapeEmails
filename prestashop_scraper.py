import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, urljoin
from collections import deque

# Leer el archivo CSV
df = pd.read_csv('prestashops_madrid_test.csv')

# Asegurarse de que estamos trabajando con la columna correcta
urls = df['Website URL'].tolist()

# Función para normalizar las URLs
def normalize_url(url):
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    parsed_url = urlparse(url)
    normalized_url = urljoin(url, '/')
    return normalized_url

# Normalizar todas las URLs
normalized_urls = [normalize_url(url) for url in urls]

# Patrones para correos electrónicos y números de teléfono
email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
phone_pattern = re.compile(r'\b\d{7,12}\b')

# Función para extraer correos y teléfonos de una página
def extract_emails_phones(content):
    emails = email_pattern.findall(content)
    phones = phone_pattern.findall(content)
    return list(set(emails)), list(set(phones))

# Función para hacer crawling y scraping
def crawl_and_scrape(base_url, max_depth=1):
    visited = set()
    queue = deque([(base_url, 0)])
    emails = set()
    phones = set()

    while queue:
        url, depth = queue.popleft()
        if depth > max_depth:
            continue
        if url in visited:
            continue
        visited.add(url)
        try:
            print(f"Visitando: {url} (profundidad: {depth})")
            response = requests.get(url, timeout=10)
            content = response.text
            new_emails, new_phones = extract_emails_phones(content)
            emails.update(new_emails)
            phones.update(new_phones)
            soup = BeautifulSoup(content, 'html.parser')
            links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('/'):
                    href = urljoin(base_url, href)
                if urlparse(href).netloc == urlparse(base_url).netloc:
                    links.append(href)
            
            # Priorizar subpáginas de contacto
            contact_links = [link for link in links if 'contact' in link.lower()]
            other_links = [link for link in links if 'contact' not in link.lower()]
            queue.extend([(link, depth + 1) for link in contact_links + other_links])
            
            print(f"Subpáginas encontradas: {len(links)}")
        except requests.RequestException as e:
            print(f"Error al acceder a {url}: {e}")

    return list(emails), list(phones)

results = []

# Ejecutar el crawler para cada URL
for i, url in enumerate(normalized_urls):
    print(f"Procesando sitio {i+1}/{len(normalized_urls)}: {url}")
    emails, phones = crawl_and_scrape(url)
    results.append({'url': url, 'emails': emails, 'phones': phones})

# Convertir los resultados en un DataFrame
results_df = pd.DataFrame(results)

# Guardar en un nuevo archivo CSV
results_df.to_csv('resultados_prestashops.csv', index=False)
