#!/usr/bin/env python

import argparse
from os import fdopen
from tempfile import mkstemp
from logging import INFO, DEBUG, WARN
from pprint import pformat
from execo import Remote, logger, Process, SshProcess, configuration, Put
from execo.log import style
from execo_g5k import get_g5k_sites, get_current_oar_jobs, get_planning,\
    compute_slots, find_free_slot, distribute_hosts, get_jobs_specs, \
    oarsub, get_oar_job_info, wait_oar_job_start, get_oar_job_nodes, \
    get_oar_job_subnets, Deployment, deploy, get_host_site, \
    default_frontend_connection_params
from execo_g5k.planning import show_resources
from execo.time_utils import format_date
from vm5k.utils import hosts_list

default_job_name = 'GoogleDataCenter'
default_walltime  = '2:00:00'
default_n_nodes = 10
default_site = 'nancy'

configuration['color_styles']['step'] = 'on_yellow', 'bold'

def main():
    logger.info('%s\n', style.step(' Launching deployment of a Google Datacenter '))
    args = set_options()
    logger.info(style.step('Retrieve Grid\'5000 resources'))
    hosts, vnet = get_resources(args.site, args.nodes, args.walltime, default_job_name)
    logger.info(style.step('Configure distem on hosts'))
    setup_distem(hosts, vnet)
    logger.info(style.log_header('Create virtual nodes'))
    

def set_options():
    """ """
    parser = argparse.ArgumentParser(description="Install OpenCloudWare on G5K")
    optio = parser.add_mutually_exclusive_group()
    optio.add_argument("-v", "--verbose",
                       action="store_true",
                       help='print debug messages')
    optio.add_argument("-q", "--quiet",
                       action="store_true",
                       help='print only warning and error messages')
    parser.add_argument("-s", "--site",
                        default=default_site,
                        help="site on which job will be run")
    parser.add_argument("-n", "--nodes",
                        type=int,
                        default=default_n_nodes,
                        help="number of nodes dedicated to OpenStack")
    parser.add_argument("-w", "--walltime",
                        default=default_walltime,
                        help="Walltime for the OAR job")
    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(DEBUG)
    elif args.quiet:
        logger.setLevel(WARN)
    else:
        logger.setLevel(INFO)
    return args


def get_resources(site=None, n_nodes=None, walltime=None, job_name=None):
    """Try to find a running job and reserve resources if needed"""
    logger.info('Looking for a running job on %s',
                style.host(site))
    job_id = None
    running_jobs = get_current_oar_jobs([site])
    for job in running_jobs:
        info = get_oar_job_info(job[0], site)
        if info['name'] == job_name:
            job_id = job[0]
            logger.info('Job %s found !', style.emph(job_id))
            break
    if not job_id:
        logger.info('Performing a new reservation')
        job_id = _make_reservation(site, n_nodes, walltime)

    logger.info('Waiting for job start ...')
    wait_oar_job_start(job_id, site)
    hosts = get_oar_job_nodes(job_id, site)
    logger.info('Hosts: %s', hosts_list(hosts))
    vnet = get_oar_job_subnets(job_id, site)[1]['ip_prefix']
    logger.info('Virtual Network: %s', vnet)
    
    return hosts, vnet


def setup_distem(hosts, vnet):
    """ """
    logger.info('Deploying hosts')
    deployed_hosts, _ = deploy(Deployment(hosts=hosts,
                                          env_name="wheezy-x64-nfs"))
    hosts = list(deployed_hosts)
    coordinator = hosts[0]
    fd, nodes_file = mkstemp(dir='/tmp/', prefix='distem_nodes_')
    f = fdopen(fd, 'w')
    f.write('\n'.join(hosts))
    f.close()
    
    Put(get_host_site(hosts[0]), [nodes_file], remote_location='/tmp/',
        connection_params={'user': default_frontend_connection_params['user']}).run()
    logger.info('Performing distem installation')    
    distem_install = SshProcess('distem-bootstrap -f ' + nodes_file,
                                get_host_site(hosts[0]),
                                connection_params={'user': 
                                                   default_frontend_connection_params['user']}).run()
    if not distem_install.ok:
        logger.error('Error in installing distem \n%s', distem_install.stdout)
    distem_vnet = SshProcess('distem --coordinator host=%s '
                             '--create-vnetwork vnetwork=vnetwork,address=%s'
                             % (coordinator, vnet),
                                coordinator).run()
    
    
    

def _make_reservation(site=None, n_nodes=None,
                      walltime=None):
    """ """
    elements = {site: n_nodes}
    logger.detail(pformat(elements))
    blacklisted = ['sagittaire']
    logger.detail('Blacklisted elements : ' + pformat(blacklisted))

    planning = get_planning(elements, subnet=True)
    slots = compute_slots(planning, walltime=walltime,
                          excluded_elements=blacklisted)
    slot = find_free_slot(slots, elements)
    logger.debug(pformat(slot))
    startdate = slot[0]
    resources = distribute_hosts(slot[2], elements,
                                 excluded_elements=blacklisted)

    show_resources(resources)    
    
    jobs_specs = get_jobs_specs(resources, name=default_job_name,
                                excluded_elements=blacklisted)
    sub, site = jobs_specs[0]
    sub.resources = 'slash_22=1+' + sub.resources
    sub.walltime = walltime
    sub.additional_options = "-t deploy"
    sub.reservation_date = startdate

    jobs = oarsub([(sub, site)])
    job_id = jobs[0][0]
    logger.info('Job %s will start at %s', style.emph(job_id),
                style.log_header(format_date(startdate)))
    
    return job_id

  
  
if __name__ == "__main__":
    main()
