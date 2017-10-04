def clean_doi(doi):
  return doi.strip().replace('\n', '').replace('\t', '').replace('"', '') if doi else None
