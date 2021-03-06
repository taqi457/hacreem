from rest_framework.views import APIView
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_200_OK
from suggestion.engine import _product_engine

class SuggestionView(APIView):

    permission_classes = (AllowAny, )

    def give_suggestion(self, data):
        result = _product_engine.product_recommendation_engine(data)
        return result

    def post(self, request, *args, **kwarg):
        data = request.DATA or request.POST
        result = self.give_suggestion(data)
        try:
            return Response(data=result, status=HTTP_200_OK)
        except Exception:
            return Response(data={}, status=HTTP_400_BAD_REQUEST)
