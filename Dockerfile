FROM python:3.10-slim


# Les paquets de l'image de base vieillissent avec le tag auquel elle est épinglée. Cette ligne
# récupère les correctifs de sécurité publiés depuis.
#
# Attention : elle ne se rejoue que si la couche est reconstruite. Le workflow de publication met
# les couches en cache (`cache-from: type=gha`), donc un Dockerfile inchangé pendant des mois
# continue de servir une couche figée. Quand une CVE corrigée apparaît sans changement de code,
# modifier l'instruction elle-même - un commentaire au-dessus ne suffit pas, il ne fait pas partie
# de la commande et n'entre pas dans la clé de cache.
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

WORKDIR /service

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# setuptools embarque ses propres copies de paquets sous `_vendor/`, et elles vieillissent : Trivy y
# voit wheel 0.45.1 et jaraco.context 5.3.0 en HIGH alors que le site-packages porte des versions
# saines. Ni pip ni la mise a jour de setuptools n'atteignent cet arbre - il n'est pas a nous.
#
# Le service tourne sur `python -m bridge.bridge` et n'a besoin ni de setuptools ni de wheel une
# fois les dependances installees (verifie : l'import du module passe sans eux). On les retire
# plutot que d'embarquer leurs CVE, comme l'image du frontend le fait pour l'arbre vendore de npm.
RUN pip uninstall -y setuptools wheel

# Copy repo contents into bridge/ subpackage so `python -m bridge.bridge` resolves
COPY . bridge/

ENV PYTHONPATH=/service

RUN useradd -m -u 1000 bridge && chown -R bridge:bridge /service
USER bridge

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health')"

CMD ["python", "-m", "bridge.bridge"]
