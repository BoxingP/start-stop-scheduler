from aws_cdk import (
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    core as cdk
)


class SchedulerStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, construct_id: str, config, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        policy_lambda = _lambda.Function.from_function_arn(
            self, 'PolicyLambda',
            function_arn=config['lambda_functions']['policy_arn']
        )
        is_start_job = sfn_tasks.LambdaInvoke(
            self, 'IsStart',
            lambda_function=policy_lambda,
            input_path='$',
            result_path='$.is_start',
            result_selector={
                'is_start.$': '$.Payload'
            },
            timeout=cdk.Duration.minutes(3)
        )

        filter_out_job = sfn_tasks.LambdaInvoke(
            self, 'FilterOutJob',
            lambda_function=_lambda.Function.from_function_arn(
                self, 'FilterOutLambda',
                function_arn=config['lambda_functions']['filter_out_arn']
            ),
            input_path='$',
            result_path='$.instance',
            result_selector={
                'instance.$': '$.Payload'
            },
            timeout=cdk.Duration.minutes(3)
        )

        process_parameter_job = sfn.Pass(
            self, 'ProcessParameters',
            input_path='$',
            parameters={
                'ec2': {
                    'is_start.$': '$.is_start.is_start',
                    'instance_type': 'ec2',
                    'instance_ids.$': '$.instance.instance.ec2'
                },
                'rds': {
                    'is_start.$': '$.is_start.is_start',
                    'instance_type': 'rds',
                    'instance_ids.$': '$.instance.instance.rds'
                }
            },
            result_path='$.parameter'
        )

        start_stop_lambda = _lambda.Function.from_function_arn(
            self, 'StartStopLambda',
            function_arn=config['lambda_functions']['start_stop_arn']
        )

        start_rds_first = sfn_tasks.LambdaInvoke(
            self, 'StartRDSFirst',
            lambda_function=start_stop_lambda,
            input_path='$.parameter.rds',
            result_path=sfn.JsonPath.DISCARD,
            timeout=cdk.Duration.minutes(15)
        )
        start_ec2_later = sfn_tasks.LambdaInvoke(
            self, 'StartEC2Later',
            lambda_function=start_stop_lambda,
            input_path='$.parameter.ec2',
            timeout=cdk.Duration.minutes(5)
        )
        stop_ec2_first = sfn_tasks.LambdaInvoke(
            self, 'StopEC2First',
            lambda_function=start_stop_lambda,
            input_path='$.parameter.ec2',
            result_path=sfn.JsonPath.DISCARD,
            timeout=cdk.Duration.minutes(5)
        )
        stop_rds_later = sfn_tasks.LambdaInvoke(
            self, 'StopRDSLater',
            lambda_function=start_stop_lambda,
            input_path='$.parameter.rds',
            timeout=cdk.Duration.minutes(15)
        )

        start_stop_succeeded = sfn.Succeed(self, 'StartStopIsSucceeded')

        start_stop_job = sfn.Choice(self, 'ConfirmSequence')
        start_stop_job.when(sfn.Condition.boolean_equals('$.is_start.is_start', True),
                            start_rds_first.next(start_ec2_later).next(start_stop_succeeded))
        start_stop_job.otherwise(stop_ec2_first.next(stop_rds_later).next(start_stop_succeeded))

        definition = sfn.Chain. \
            start(is_start_job).next(filter_out_job).next(process_parameter_job).next(start_stop_job)

        start_stop_machine = sfn.StateMachine(self, 'StartStopMachine', definition=definition,
                                              timeout=cdk.Duration.minutes(20))

        scheduler_event = events.Rule(
            self, "ScheduleRule",
            description='To trigger the start-stop operations for specific instances',
            rule_name='-'.join([construct_id, 'rule'.replace(' ', '-')]),
            schedule=events.Schedule.expression(config['cron']),
            targets=[
                targets.SfnStateMachine(
                    start_stop_machine,
                    input=events.RuleTargetInput.from_object(config['input'])
                )
            ]
        )
