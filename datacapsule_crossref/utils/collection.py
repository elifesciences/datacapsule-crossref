def iter_dict_to_list(iterable, fields):
  return (
    [item.get(field) for field in fields]
    for item in iterable
  )
