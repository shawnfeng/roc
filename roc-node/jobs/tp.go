package jobs


type jobCmdType int32
const (
	_                        = iota
	JOBCMD_START  jobCmdType = iota
	JOBCMD_STOP

	JOBCMD_KILL
)

func (m jobCmdType) String() string {
	s := "UNKNOWN"

	switch m {
	case JOBCMD_START:
		s = "JOBCMD_START"
	case JOBCMD_STOP:
		s = "JOBCMD_STOP"
	case JOBCMD_KILL:
		s = "JOBCMD_KILL"
	}


	return s
}

type jobLoopType int32

const (
	_                        = iota
	Loop_STOP    jobLoopType = iota

	Loop_RUN
	Loop_RUNCHEACK
	Loop_BACKOFF

)

type jobExitType int32
const (
	_                     = iota
	Exit_OK   jobExitType = iota
	Exit_Err_StdoutPipe
	Exit_Err_CmdStart

	Exit_Err_Kill
	Exit_Err_JobOtherErr

)



func (m jobExitType) String() string {
	s := "UNKNOWN"

	switch m {
	case Exit_OK:
		s = "OK"

	case Exit_Err_StdoutPipe:
		s = "Err_StdoutPipe"
	case Exit_Err_CmdStart:
		s = "Err_CmdStart"

	case Exit_Err_Kill:
		s = "Err_Kill"

	case Exit_Err_JobOtherErr:
		s = "Err_JobOtherErr"
	}

	return s
}

type jobLoopType int32

const (
	_                        = iota
	Loop_STOP    jobLoopType = iota

	Loop_RUN
	Loop_RUNCHEACK
	Loop_BACKOFF

)

func (m jobLoopType) String() string {
	s := "UNKNOWN"

	if Loop_STOP == m {
		s = "STOP"

	} else if Loop_RUN == m {
		s = "RUN"

	} else if Loop_RUNCHEACK  == m {
		s = "RUNCHEACK"

	} else if Loop_BACKOFF == m {
		s = "BACKOFF"

	}

	return s
}

type runNotify struct {
	pidJob int
}

type exitState struct {
	runDuration time.Duration
	exitType jobExitType
}


type userCommand struct {

	cmd jobCmdType
	// callback
}


