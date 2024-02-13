from flask_restx import Namespace, Resource, fields
from flask import Response, request
from flask_restful import reqparse
from src.controller import create_report
import json

api = Namespace('report', description='Service to generate report')

pr_input_model={}
pr_input_model.update({'request_report': api.model('report/request: input',
		   { 
             'tenant_id': fields.String(required=True, example="2130c125-6167-583a-a931-a62079c4a064"),
             'start_date': fields.Date(required=False, example="2023-08-01", dt_format='rfc822'),
             'end_date': fields.Date(required=False, example="2023-10-01", dt_format='rfc822'),
        })
})

pr_output_model={}
pr_output_model.update({'request_report': api.model('report/request: output',
		   { 
             'report_id': fields.String(required=True, example="651125cd-0012-51b4-a335-886c4b0716a6",description='Report ID using UUID format')
        })
})


class ReportEndpoint(Resource):

    def request_report(self, data):
        status = 200
        ok, res = create_report(data)

        if not ok:
            status=406

        return Response(json.dumps(json.loads(res)), status, mimetype='application/json')


@api.route("/request")
class RequestReportEndpoint(ReportEndpoint):

    @api.response(200, 'Success', pr_output_model['request_report'])
    @api.doc(body=pr_input_model['request_report'], 
                deprecated=False, 
                description="This route is being used to return an invoice report.",
                responses={ 200: 'Success', 406: 'Invalid Argument' })
    
    def post(self):
        args = self.__post_params()
        data = {'tenant_id':args['tenant_id'], 
                'start_date':args['start_date'],
                'end_date':args['end_date']
                }
        
        return self.request_report(data)

    def __post_params(self):
        parser = reqparse.RequestParser(bundle_errors=True, trim=True)
        parser.add_argument('tenant_id', required=True, type=str, location=['json'],trim=True)
        parser.add_argument('start_date', required=False, type=str, location=['json'],trim=True)
        parser.add_argument('end_date', required=False, type=str, location=['json'],trim=True)
        
        return parser.parse_args()