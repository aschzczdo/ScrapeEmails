import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, urljoin
from collections import deque
import time

# Leer el archivo CSV
file_path = "woocommerce-no-emails.csv"
result_path = file_path.replace(".csv", "_result.csv")
df = pd.read_csv(file_path)
# Asegurarse de que estamos trabajando con la columna correcta
urls = df["Website URL"].tolist()


# Función para normalizar las URLs
def normalize_url(url):
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    parsed_url = urlparse(url)
    normalized_url = urljoin(url, "/")
    return normalized_url


# Normalizar todas las URLs
normalized_urls = [normalize_url(url) for url in urls]

# Patrones para correos electrónicos y números de teléfono
email_pattern = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
phone_pattern = re.compile(r"\b(?:0|\+|6|7|9)\d{6,11}\b")


# Función para extraer correos y teléfonos de una página
def extract_emails_phones(content):
    emails = email_pattern.findall(content)
    phones = phone_pattern.findall(content)
    return list(set(emails)), list(set(phones))


# Patrones de palabras clave para priorizar enlaces relevantes
priority_patterns = [
    re.compile(r"contact", re.IGNORECASE),
    re.compile(r"about", re.IGNORECASE),
    re.compile(r"about-us", re.IGNORECASE),
    re.compile(r"contact-us", re.IGNORECASE),
    re.compile(r"contactanos", re.IGNORECASE),
    re.compile(r"quienes-somos", re.IGNORECASE),
    re.compile(r"terms-and-conditions", re.IGNORECASE),
    re.compile(r"terminos-y-condiciones", re.IGNORECASE),
    re.compile(r"terminos", re.IGNORECASE),
    re.compile(r"policies", re.IGNORECASE),
    re.compile(r"politicas", re.IGNORECASE),
    re.compile(r"privacy", re.IGNORECASE),
    re.compile(r"privacidad", re.IGNORECASE),
    re.compile(r"terminos-de-uso", re.IGNORECASE),
    re.compile(r"aviso-legal", re.IGNORECASE),
    re.compile(r"legal", re.IGNORECASE),
    re.compile(r"aviso", re.IGNORECASE),
    re.compile(r"politica-de-privacidad", re.IGNORECASE),
    re.compile(r"politica", re.IGNORECASE),
]


# Función para hacer crawling y scraping
def crawl_and_scrape(base_url, max_depth=3, max_pages_per_depth=15):
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
            if "text/html" not in response.headers.get("Content-Type", ""):
                print(f"Ignorado {url} (contenido no HTML)")
                continue
            content = response.text
            new_emails, new_phones = extract_emails_phones(content)
            emails.update(new_emails)
            phones.update(new_phones)

            # Si ya tenemos un correo y un teléfono, dejamos de buscar
            if emails and phones:
                print(
                    f"Encontrado al menos un correo y un teléfono en {url}. Deteniendo búsqueda."
                )
                return list(emails), list(phones)

            soup = BeautifulSoup(content, "html.parser")
            links = []
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if href.startswith("/"):
                    href = urljoin(base_url, href)
                if urlparse(href).netloc == urlparse(base_url).netloc:
                    links.append(href)

            # Priorizar subpáginas relevantes
            priority_links = [
                link
                for link in links
                if any(pattern.search(link) for pattern in priority_patterns)
            ]
            non_priority_links = [link for link in links if link not in priority_links]
            links_to_visit = priority_links + non_priority_links

            # Limitar el número de subpáginas a visitar por nivel de profundidad
            if len(links_to_visit) > max_pages_per_depth:
                links_to_visit = links_to_visit[:max_pages_per_depth]

            queue.extend([(link, depth + 1) for link in links_to_visit])

            print(f"Subpáginas encontradas: {len(links_to_visit)}")

            # Añadir una pausa para evitar ser bloqueado por el servidor
            time.sleep(1)
        except requests.RequestException as e:
            print(f"Error al acceder a {url}: {e}")

    return list(emails), list(phones)


results = []

# Ejecutar el crawler para cada URL
for i, url in enumerate(normalized_urls):
    print(f"Procesando sitio {i+1}/{len(normalized_urls)}: {url}")
    emails, phones = crawl_and_scrape(url)
    results.append({"url": url, "emails": emails, "phones": phones})

# Convertir los resultados en un DataFrame
results_df = pd.DataFrame(results)

# Guardar en un nuevo archivo CSV
results_df.to_csv(result_path, index=False)
