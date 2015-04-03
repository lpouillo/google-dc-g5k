# google-dc-g5k

Setup a virtual datacenter using execo and distem and injects faults into the system, 
using Google cluster data events. 


## Installation

This module requires execo > 2.4.3.
  
  export http_proxy="http://proxy:3128" ; export https_proxy="https://proxy:3128" 
  easy_install --user execo
  easy_install --user requests
  
  git clone https://github.com/lpouillo/google-dc-g5k.git
  

## Usage

  google_dc_g5k deploy 1000
  
will deploy 1000 virtual nodes.

  google_dc_g5k play 4:00:00
  
will play the events during 4 hours on the virtual nodes.
 

## Licence 
