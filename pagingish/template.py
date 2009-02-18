

            
        



class RenderStuff(object):

    def render_paging(self, request):

        paging_data = 

        if paging_data.page_count == 1:
            return ''
        
        previous_tag = inevow.IQ(tag).onePattern('previous')
        next_tag = inevow.IQ(tag).onePattern('next')

        tag.fillSlots('count','%d items'%paging_data.item_count)

        if paging_data.page_number > 1:
            previous_tag(href=url.URL.fromContext(ctx).replace('page', paging_data.page_number - 1))
            tag.fillSlots('previous',previous_tag)
        else:
            tag.fillSlots('previous','')

        if paging_data.page_number < paging_data.page_count:
            next_tag(href=url.URL.fromContext(ctx).replace('page', paging_data.page_number + 1))
            tag.fillSlots('next',next_tag)
        else:
            tag.fillSlots('next','')

        return tag

    def render_ranges(self,ctx,data):
        tag = ctx.tag
        paging_data = ctx.locate(IPagingData)
        current = inevow.IQ(tag).patternGenerator('range-current')
        not_current = inevow.IQ(tag).patternGenerator('range-not_current')
        page_count = paging_data.page_count
        page_size = paging_data.page_size
        page_number = paging_data.page_number

        page_per_side = 3

        paging_left = T.div(id='paging-range-left')
        paging_center = T.div(id='paging-range-center')
        paging_right = T.div(id='paging-range-right')

        for page in range( page_number-(page_per_side+1), page_number+(page_per_side+1)+1 ):
            position = page-page_number
            start_position = False
            end_position = False

            # Is the position at the start or the end
            if position == -(page_per_side+1):
                start_position = True
            if position  == (page_per_side+1):
                end_position = True

            # is the current slot within the page range
            if page < 1:
                withinRange = False
            elif page > page_count:
                withinRange = False
            else:
                withinRange = True

            # If it's the start or end position and that position
            # is within the page range then add the 'dots
            if start_position and withinRange:
                paging_left[ ' ', T.a(href=url.URL.fromContext(ctx).replace('page', 1))['...'], ' ' ]
                continue
            if end_position and withinRange:
                paging_right[ ' ', T.a(href=url.URL.fromContext(ctx).replace('page', page_count))['...'], ' ' ]
                continue

            if withinRange:

                if position < 0:
                    pattern = not_current()
                    pattern.fillSlots('rangestring',page)
                    pattern.fillSlots('rangeurl',url.URL.fromContext(ctx).replace('page', page))
                    paging_left[ pattern ]

                if position > 0:
                    pattern = not_current()
                    pattern.fillSlots('rangestring',page)
                    pattern.fillSlots('rangeurl',url.URL.fromContext(ctx).replace('page', page))
                    paging_right[ pattern ]

                if position == 0:
                    pattern = current()
                    pattern.fillSlots('rangestring',page)
                    paging_center[ pattern ]

        return tag[ paging_left, paging_center, paging_right ]

