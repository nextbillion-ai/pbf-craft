# Rosmosis

## Basic usage

Find an element

`rosmosis search -f ./singapore.osm.pbf --eltype way --elid 1005871625`

Find an element and the elements associated with it

`rosmosis search -f ./singapore.osm.pbf --eltype way --elid 1005871625 -e false`

Find elements by tag

`rosmosis search -f ./singapore.osm.pbf --tagkey highway --tagvalue motorway`

Find elements by a node pair

`rosmosis search -f ./singapore.osm.pbf --pair 100052832 --pair 100003223`

For help

`rosmosis help`
