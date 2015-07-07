# Voyager Python Client

A library that exposes functions available via Voyagers core rest api.

## Requirements

* Python 2.7
* [Requests](http://docs.python-requests.org/)

## Using

Quick sample of using the library.

    import voyager

    # connect
    cli = voyager.Client()

    # list locations
    for l in cli.locations():
      print l

    # get a location by id
    l = cli.get_location('theid')

    # add a new location
    l = cli.add_location(voyager.Path('Spatial Files', 'C:\geodata\'))
    # l = cli.add_location(voyager.PostGIS('geo', 'countries'))
    # l = cli.add_location(voyager.Feed())

    # index the new location
    l.index()

