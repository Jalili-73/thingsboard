# thingsboard
thingsboard customization<br/>
handle complex telemetry in thingsboard<br/>
assuming that device send json like this<br/>
{"data_list:[
{
  "id":3,
  "v":3,
  "s":3,
  "err":[3,3],
  "vlt":3,
  "cur":[3,3,3,3,3],
  "bat_tmp":[3,3,3,3,3,3],
  "cell_vlt":[3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,4,4,4],
  "blc_s":3,
  "so":[3,3],
  "rem":[3,3],
  "chg":[3,3],
  "od":3,
  "spd":[3,3],
  "tmp":[3,3,3],
  "loc":[3,3],
  "sim":[3,3,3],
  "cns":3,
  "t":"3"
},
{
  "id":3,
  "v":3,
  "s":3,
  "err":[3,3],
  "vlt":3,
  "cur":[3,3,3,3,3],
  "bat_tmp":[3,3,3,3,3,3],
  "cell_vlt":[3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,4,4,4],
  "blc_s":3,
  "so":[3,3],
  "rem":[3,3],
  "chg":[3,3],
  "od":3,
  "spd":[3,3],
  "tmp":[3,3,3],
  "loc":[3,3],
  "sim":[3,3,3],
  "cns":3,
  "t":"3"
}]
}
<br/>the length of array is between 1 and 10<br/>
we want change to the thingsboard code to handle it<br/>
