
# clean DOI without changing it too much
def clean_doi(doi):
  return doi.strip().replace('\n', '').replace('\t', '') if doi else None

def doi_to_normalised_key(doi):
  return (
    doi
    .strip()
    .lower()
    .replace('\n', '')
    .replace('\t', '')
    .replace('"', '')
    .replace('=', '/')
  ) if doi else None
