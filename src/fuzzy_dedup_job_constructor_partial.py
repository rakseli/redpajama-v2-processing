
import subprocess
import os
import time
import argparse

#This script can be used to run dedup for:
#       all languages: python fuzzy_dedup_job_constructor.py --all
#       a single language: python fuzzy_dedup_job_constructor.py --lang de

#FR estimated file size 260G
#IT estimated file size 150G
#DE estimated file size 300G
#ES estimated file size 300G


parser = argparse.ArgumentParser()
parser.add_argument("--all",help="whether run dedup for all",action='store_true')
parser.add_argument("--lang",type=str,help="if running single job, what lang to use",default='it')

def create_slurm_scripts(script_name,lang,log_path="/scratch/project_462000353/akselir/redpajama-v2/fuzzy_dedup_logs"
                        ,account="project_462000444"
                        ,cpus_per_task=1,time="00:05:00"
                        ,mem_per_cpu=500,partition='small', shard=0,iteration=1):
    """Creates a slurm script in right string format

    Args:
        script_name (str): name for log files and slurm 
        lang (str): lang to dedup
        log_path (str): path where logs are saved. Defaults to "/scratch/project_462000353/akselir/redpajama-v2/fuzzy_dedup_logs".
        account (str): billing account Defaults to "project_462000444".
        cpus_per_task (int): n cpus. Defaults to 1.
        time (str): running time give in HH:MM:SS format. Defaults to "00:05:00".
        mem_per_cpu (int): mem per cpu in mb. Defaults to 100.
        partition (str): partition to run the scirpt. Defaults to 'small'.
    Returns:
    - str: the script
    """    
    
    script_content = f"""#!/bin/bash
#SBATCH --job-name={script_name}
#SBATCH --output={log_path}/{script_name}_%j.out
#SBATCH --error={log_path}/{script_name}_%j.err
#SBATCH --account={account}
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --time={time}
#SBATCH --mem-per-cpu={mem_per_cpu}
#SBATCH --partition={partition}
echo "Script launched with SLURM parameters:"
echo "--job-name={script_name}"
echo "--output={log_path}/{script_name}_%j.out"
echo "--error={log_path}/{script_name}_%j.err"
echo "--account={account}"
echo "--cpus-per-task={cpus_per_task}"
echo "--time={time}"
echo "--mem-per-cpu={mem_per_cpu}"
echo "--partition={partition}"
echo "Script launched with program parameter:"
echo "Language: {lang}"
module purge
# singularity setup
CONTAINER="/scratch/project_462000353/akselir/containers/preprocessing_container.sif"
SING_BIND="/scratch/project_462000353/"
srun \
    singularity exec \
    -B "$SING_BIND" \
    "$CONTAINER" \
    python /scratch/project_462000353/akselir/redpajama-v2/src/minhashlsh_partial_round_2.py --shard {shard} --iteration {iteration} --lang {lang}
""" 

    return script_content


if __name__ == "__main__":
    args = parser.parse_args()    
    iteration = 2
    if args.all:
        for l in ["en","fr","es","de","it"]:
            for i in range(0,2):
                if l == 'fr':
                   s = create_slurm_scripts(f"partial_{l}_small_fuzzy_dedup_iteration_{iteration}",lang=l,cpus_per_task=3,time='36:00:00',mem_per_cpu=216666,partition='small',shard=i,iteration=iteration)
                elif l == 'it':
                   s = create_slurm_scripts(f"partial_{l}_small_fuzzy_dedup_iteration_{iteration}",lang=l,cpus_per_task=3,time='36:00:00',mem_per_cpu=216666,partition='small',shard=i,iteration=iteration)
                elif l=='de':
                   s = create_slurm_scripts(f"partial_{l}_small_fuzzy_dedup_iteration_{iteration}",lang=l,cpus_per_task=3,time='36:00:00',mem_per_cpu=216666,partition='small',shard=i,iteration=iteration)
                elif l=='es':
                   s = create_slurm_scripts(f"partial_{l}_small_fuzzy_dedup_iteration_{iteration}",lang=l,cpus_per_task=3,time='36:00:00',mem_per_cpu=216666,partition='small',shard=i,iteration=iteration)
                elif l=='en':
                   s = create_slurm_scripts(f"partial_{l}_small_fuzzy_dedup_iteration_{iteration}",lang=l,cpus_per_task=3,time='36:00:00',mem_per_cpu=216666,partition='small',shard=i,iteration=iteration)
                temp_file_name = f"{os.getcwd()}/slurm_job_{l}_shard_{i}.sh"
                with open(temp_file_name,"w") as temp_file:
                    temp_file.write(s)
                    # Submit the SLURM job using sbatch with the temporary file
                subprocess.run(["sbatch", temp_file_name], text=True)
                time.sleep(1)
                os.remove(temp_file_name)
    else:
        l = args.lang
        for i in range(0,2):
            s = create_slurm_scripts(f"round_2_partial_{l}_small_fuzzy_dedup_iteration_{iteration}",lang=l,cpus_per_task=3,time='12:00:00',mem_per_cpu=216666,partition='small',shard=i,iteration=iteration)
            temp_file_name = f"{os.getcwd()}/slurm_job_{l}_shard_{i}.sh"
            with open(temp_file_name,"w") as temp_file:
                temp_file.write(s)
                # Submit the SLURM job using sbatch with the temporary fil14200e
            subprocess.run(["sbatch", temp_file_name], text=True)
            time.sleep(1)
            os.remove(temp_file_name)
