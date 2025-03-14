# Deadline Worker Scheduler

* You want to render on artist's workstations, but there is no regularity in artist's schedule?
* Deadline idle detection doesn't work, because you are running Deadline as a service?
* Deadline worker scheduling is not enough?

This command line utility tries to solve worker scheduling by supplying team attendance csv file.
Deadline worker description is used for artist <-> machine assignment.
Deadline worker comment is used as an output of the script.

## The CSV file
The csv (utf-8) file name is the date in form YYMMDD.csv.
Script finds closest date (csv file) at given folder.
Two columns, with no column titles are expected:
* artist name
* status

There are special values for status:
OFF, PAUSED or <empty> is considered inactive status (ie workstation is free for rendering)
Any other value is considered active

Typical lines:
* John Doe,amazing project
* Jane Doe,OFF
* Peter Pan,PAUSED
* Adam Empty,

## The Deadline Description
Two to four space separated values:
1. Worker type (R for render node, W for workstation)
2. CPU or GPU (cpu or gpu, colon to separate the spec)
3. Username (ascii only, dots instead of space)
4. User Occupation (department or job title)

Typical names:
* R cpu
* R gpu:4090
* W gpu:4080s john.doe anim

## The Deadline Comment
Comment is one of the outputs of the script. Only the first letter is used for setting up (enabling or disabling) of the workers.
The comments:

* Render Node
* Ignore - Machine
* Ignore - User
* Paused - User not active
* Free Workstation
* Workstation in use in working hours

## What machines should be enabled for rendering?
Any machine that is render node, free workstation, or with paused artist
Workers with Ignore comment should be disabled.
The Workstation in use in working hours comment can be controlled by the command line argument.
It can enable or disable rendering (maybe depending on the time script is run...)

## Artist name matching
* All non-ascii characters are changed toi closest ascii equivalent
* Spaces are replaced by dot(s)
* Match is case-insensitive

## The Setup Json
Configuration file, expected to be in script current directory
If first character of the path is dot, the dot is replaced with current script directory.
* "path_team": Folder with csv files containing team attendance
* "path_ignore_people": Text file where each line contains user name to be ignored (his machine will not render)
* "path_ignore_workers": Text file where each line contains machine to be ignored (machine will not render)
* "worker_info_folder": Folder where script exports json files with details pulled from the Deadline

## The Command Line Arguments

### --dry
Skips any operation that would change Deadline.
### --comments_only
Sets comments in Deadline but doesn't enable/disable workers.
### --use_comments
Enables / disable workers by commments in Deadline only. No Team attendance or ignore files.
### --workstations_render
Enable worker on workstations. For off-hours.
### --check
After enabling / disabling workers, read back workers status and report success/failure
### --log_level
Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL
