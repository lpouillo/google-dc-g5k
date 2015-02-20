# google-dc-g5k

Setup a virtual datacenter using execo and distem and injects faults into the system, 
using various events generator (googleclusterdata, 


= Installation =
This module requires execo > 2.4.3.
  
  export http_proxy="http://proxy:3128" ; export https_proxy="https://proxy:3128" 
  easy_install --user execo
  git clone https://github.com/lpouillo/google-dc-g5k.git
  

= Usage =

  google-dc-g5k.git -s lyon -np 20 -nv 500

= Copyright =
