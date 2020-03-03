# pageviews
<h5>What additional things would you want to operate this application in a production setting?</h5>

Assuming there's more functionality to justify the below.

1. Configurability
    
    Lot of hardcoded things which you'd not want in a production environment. First thing would be to move those configs out to be managed by something like puppet.
2. Retries

    Better exception handling, could improve on te logic of retrying when different failures happen.
    
3. High Availability

    Distribute the service across different VM's or kubernetify it
    
4. Switch out the execution engine

    I wouldn't use a thread pool to execute the downloads/process tasks. These are ideal candidates for some form of distributed execution engine such as spark that abstracts away the complexities of auto scaling (if with kubernetes), scheduling and task orchestration.

5. Move cached results to S3

    I/O interactions are normally complicated, S3 gives you durability, redundancy and atomic writes that I'd usually avoid if possible. Though you are paying a network cost here.

     
<h5>What might change about your solution if this application needed to run automatically for each hour of the day?</h5>

I'd probably look into integrating some form of job scheduler like jenkins to auto run this. At a larger scale, I'd see if 

How would you test this application?

How you’d improve on this application design?